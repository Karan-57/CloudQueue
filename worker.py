import sqlite3
import time
import os
import sys
import argparse
import random
import json
import signal
import config

def _handle_sigterm(signum, frame):
    raise KeyboardInterrupt

try:
    signal.signal(signal.SIGTERM, _handle_sigterm)
except Exception:
    pass

DB = config.DATABASE_PATH


def get_db():
    db_path = config.DATABASE_PATH
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.isolation_level = None
    return conn


def claim_job(conn, worker_id, experiment_id=None):
    table = "experiment_jobs" if experiment_id else "jobs"
    where_sub = "experiment_id = ? AND status = 'Queued'" if experiment_id else "status = 'Queued'"
    now = time.time()
    params = (now, worker_id, experiment_id) if experiment_id else (now, worker_id)

    for attempt in range(10):
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            cursor.execute(f"""
                UPDATE {table}
                SET status = 'Processing',
                    started_at = ?,
                    worker_id = ?
                WHERE id = (
                    SELECT id FROM {table}
                    WHERE {where_sub}
                    ORDER BY id ASC
                    LIMIT 1
                )
                RETURNING id, job_name, workload_type, submitted_at
            """, params)

            job = cursor.fetchone()
            conn.execute("COMMIT")
            return job
        except sqlite3.OperationalError as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(random.uniform(0.02, 0.08))
            else:
                raise e
    return None


def claim_specific_job(conn, job_id, worker_id, experiment_id=None):
    table = "experiment_jobs" if experiment_id else "jobs"
    where_sub = "WHERE id = ? AND experiment_id = ?" if experiment_id else "WHERE id = ?"
    check_params = (job_id, experiment_id) if experiment_id else (job_id,)

    for attempt in range(10):
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            now = time.time()

            cursor.execute(f"SELECT status, job_name, workload_type, submitted_at FROM {table} {where_sub}", check_params)
            row = cursor.fetchone()
            if not row:
                conn.execute("COMMIT")
                return ("NOT_FOUND", None)

            current_status = row[0]
            if current_status == "Completed":
                conn.execute("COMMIT")
                return ("ALREADY_COMPLETED", None)
            elif current_status == "Processing":
                conn.execute("COMMIT")
                return ("ALREADY_PROCESSING", None)
            elif current_status != "Queued":
                conn.execute("COMMIT")
                return ("OTHER_STATUS", current_status)

            where_claim = "WHERE id = ? AND experiment_id = ? AND status = 'Queued'" if experiment_id else "WHERE id = ? AND status = 'Queued'"
            claim_params = (now, worker_id, job_id, experiment_id) if experiment_id else (now, worker_id, job_id)

            cursor.execute(f"""
                UPDATE {table}
                SET status = 'Processing',
                    started_at = ?,
                    worker_id = ?
                {where_claim}
                RETURNING id, job_name, workload_type, submitted_at
            """, claim_params)

            claimed = cursor.fetchone()
            conn.execute("COMMIT")
            return ("CLAIMED", claimed) if claimed else ("ALREADY_PROCESSING", None)

        except sqlite3.OperationalError as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(random.uniform(0.02, 0.08))
            else:
                raise e

    return ("LOCKED", None)


def execute_workload(workload_type):
    start_time = time.time()
    if workload_type == "CPU":
        total = 0
        for i in range(1, 1000000):
            total += i * i
    elif workload_type == "DATA":
        data = list(range(100000))
        data.sort()
    elif workload_type == "TEXT":
        text = "CloudQueue workload processing " * 10000
        text = text.lower()
        text = text.upper()
    else:
        time.sleep(2)
    return start_time, time.time()


def complete_job(conn, job_id, submitted_at, started_at, completed_at, experiment_id=None):
    table = "experiment_jobs" if experiment_id else "jobs"
    processing_time = round(completed_at - started_at, 3)
    waiting_time = round(started_at - submitted_at, 3) if submitted_at else 0.0
    total_latency = round(completed_at - submitted_at, 3) if submitted_at else processing_time

    for attempt in range(10):
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(f"""
                UPDATE {table}
                SET status = 'Completed',
                    completed_at = ?,
                    processing_time = ?,
                    waiting_time = ?,
                    total_latency = ?
                WHERE id = ?
            """, (completed_at, processing_time, waiting_time, total_latency, job_id))
            conn.execute("COMMIT")
            return processing_time, waiting_time, total_latency
        except sqlite3.OperationalError as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(random.uniform(0.02, 0.08))
            else:
                raise e

    return processing_time, waiting_time, total_latency


def _count_queued(conn, table, experiment_id=None):
    cursor = conn.cursor()
    if experiment_id:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE experiment_id = ? AND status = 'Queued'", (experiment_id,))
    else:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE status = 'Queued'")
    return cursor.fetchone()[0]


def run_gcp_worker(worker_id, poll_interval=0.5, exit_when_empty=False, experiment_id=None, worker_mode="normal"):
    context_label = f"[{worker_id}" + (f":{experiment_id}]" if experiment_id else "]")
    print(f"{context_label} GCP Pub/Sub Worker started (PID: {os.getpid()}).")
    print(f"{context_label} Worker mode: {worker_mode}")
    print(f"{context_label} Project: {config.GOOGLE_CLOUD_PROJECT or '(mock/local)'}, Sub: {config.PUBSUB_SUBSCRIPTION}")
    print(f"{context_label} Target Queue: {'experiment_jobs (' + experiment_id + ')' if experiment_id else 'jobs (standard)'}")

    import pubsub_client

    conn = get_db()
    jobs_processed = 0
    empty_notified = False

    try:
        while True:
            messages = pubsub_client.pull_messages(max_messages=1, timeout=2.0)

            if not messages:
                if exit_when_empty:
                    table = "experiment_jobs" if experiment_id else "jobs"
                    if _count_queued(conn, table, experiment_id) == 0:
                        time.sleep(0.2)
                        if _count_queued(conn, table, experiment_id) == 0:
                            print(f"{context_label} Queue empty in SQLite and no Pub/Sub messages. Exiting (--exit-when-empty enabled).")
                            break
                else:
                    if not empty_notified:
                        print(f"{context_label} Pub/Sub subscription is idle. Standing by...")
                        empty_notified = True
                    time.sleep(poll_interval)
                continue

            empty_notified = False
            msg_wrapper = messages[0]
            ack_id = msg_wrapper.ack_id
            raw_data = msg_wrapper.message.data if hasattr(msg_wrapper, "message") else msg_wrapper.data

            try:
                payload_str = raw_data.decode("utf-8") if isinstance(raw_data, bytes) else str(raw_data)
                payload = json.loads(payload_str)
            except Exception:
                print(f"{context_label} Warning: Received malformed Pub/Sub message data: {raw_data}. Safely ACKing to discard.")
                pubsub_client.acknowledge_message(ack_id)
                continue

            if not isinstance(payload, dict) or "job_id" not in payload:
                print(f"{context_label} Warning: Pub/Sub message missing 'job_id': {payload}. Safely ACKing.")
                pubsub_client.acknowledge_message(ack_id)
                continue

            job_id = payload.get("job_id")
            msg_exp_id = payload.get("experiment_id")

            if experiment_id and msg_exp_id != experiment_id:
                pubsub_client.nack_message(ack_id)
                time.sleep(0.05)
                continue

            if not experiment_id and msg_exp_id:
                pubsub_client.nack_message(ack_id)
                time.sleep(0.05)
                continue

            status_code, job_data = claim_specific_job(conn, job_id, worker_id, experiment_id=experiment_id)

            if status_code == "ALREADY_COMPLETED":
                print(f"{context_label} Job #{job_id} already Completed in DB (redelivery). Safely ACKing message.")
                pubsub_client.acknowledge_message(ack_id)
                continue
            elif status_code == "ALREADY_PROCESSING":
                print(f"{context_label} Job #{job_id} currently Processing by another worker. NACKing message.")
                pubsub_client.nack_message(ack_id)
                time.sleep(0.05)
                continue
            elif status_code == "NOT_FOUND":
                print(f"{context_label} Warning: Job #{job_id} not found in DB. Safely ACKing to clear stale message.")
                pubsub_client.acknowledge_message(ack_id)
                continue
            elif status_code != "CLAIMED":
                print(f"{context_label} Could not claim Job #{job_id} (status: {status_code}). NACKing.")
                pubsub_client.nack_message(ack_id)
                time.sleep(0.05)
                continue

            claimed_id, job_name, workload_type, submitted_at = job_data
            wait_time_preview = round(time.time() - submitted_at, 3) if submitted_at else 0.0
            print(f"{context_label} Claimed Job #{claimed_id} ({workload_type}) via Pub/Sub | Waiting: {wait_time_preview}s")

            work_start, work_end = execute_workload(workload_type)
            p_time, w_time, t_lat = complete_job(
                conn, claimed_id, submitted_at, work_start, work_end, experiment_id=experiment_id
            )

            pubsub_client.acknowledge_message(ack_id)

            jobs_processed += 1
            print(f"{context_label} Done Job #{claimed_id} & ACKed | Proc: {p_time}s | Wait: {w_time}s | Total: {t_lat}s")

    except KeyboardInterrupt:
        print(f"\n{context_label} Shutting down gracefully...")
    finally:
        conn.close()
        print(f"{context_label} Stopped. Total jobs processed: {jobs_processed}")


def run_worker(worker_id, poll_interval=0.5, exit_when_empty=False, experiment_id=None, mode=None, worker_mode="normal"):
    if mode is None:
        mode = config.CLOUDQUEUE_MODE

    context_label = f"[{worker_id}" + (f":{experiment_id}]" if experiment_id else "]")
    print(f"{context_label} Worker started (PID: {os.getpid()}) in '{mode}' mode.")
    print(f"{context_label} Worker mode: {worker_mode}")

    if mode == "gcp":
        run_gcp_worker(
            worker_id=worker_id,
            poll_interval=poll_interval,
            exit_when_empty=exit_when_empty,
            experiment_id=experiment_id,
            worker_mode=worker_mode
        )
        return

    conn = get_db()
    jobs_processed = 0
    empty_notified = False

    target_queue_name = f"experiment_jobs ({experiment_id})" if (worker_mode == "experiment" and experiment_id) else "jobs (standard)"
    print(f"{context_label} Target Queue: {target_queue_name}")

    try:
        while True:
            claim_exp_id = experiment_id if worker_mode == "experiment" else None
            job = claim_job(conn, worker_id, experiment_id=claim_exp_id)

            if job is not None:
                empty_notified = False
                job_id, job_name, workload_type, submitted_at = job
                wait_time_preview = round(time.time() - submitted_at, 3) if submitted_at else 0.0
                print(f"{context_label} Claimed Job #{job_id} ({workload_type}) | Waiting: {wait_time_preview}s")

                work_start, work_end = execute_workload(workload_type)
                p_time, w_time, t_lat = complete_job(
                    conn, job_id, submitted_at, work_start, work_end, experiment_id=claim_exp_id
                )

                jobs_processed += 1
                print(f"{context_label} Done Job #{job_id} | Proc: {p_time}s | Wait: {w_time}s | Total: {t_lat}s")

            else:
                if exit_when_empty:
                    time.sleep(0.15)
                    job_retry = claim_job(conn, worker_id, experiment_id=claim_exp_id)
                    if job_retry is None:
                        print(f"{context_label} Queue empty. Exiting (--exit-when-empty enabled).")
                        break
                    else:
                        job_id, job_name, workload_type, submitted_at = job_retry
                        work_start, work_end = execute_workload(workload_type)
                        complete_job(conn, job_id, submitted_at, work_start, work_end, experiment_id=claim_exp_id)
                        jobs_processed += 1
                else:
                    if not empty_notified:
                        print(f"{context_label} Queue is idle. Standing by...")
                        empty_notified = True
                    time.sleep(poll_interval)

    except KeyboardInterrupt:
        print(f"\n{context_label} Shutting down gracefully...")
    finally:
        conn.close()
        print(f"{context_label} Stopped. Total jobs processed: {jobs_processed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CloudQueue Background Worker")
    parser.add_argument(
        "--worker-id",
        type=str,
        default=os.environ.get("WORKER_ID", f"worker-{os.getpid()}"),
        help="Unique identifier for this worker instance"
    )
    parser.add_argument(
        "--worker-mode",
        type=str,
        default=None,
        choices=["normal", "experiment"],
        help="Worker mode: 'normal' for standard jobs, 'experiment' for isolated experiment jobs"
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.5,
        help="Polling delay in seconds when queue is empty (default: 0.5s)"
    )
    parser.add_argument(
        "--exit-when-empty",
        action="store_true",
        help="Exit automatically when no queued jobs remain"
    )
    parser.add_argument(
        "--experiment-id",
        type=str,
        default=None,
        help="Optional experiment ID to isolate job claiming to experiment_jobs"
    )
    parser.add_argument(
        "--mode",
        type=str,
        default=config.CLOUDQUEUE_MODE,
        choices=["local", "gcp"],
        help="Queue mode: 'local' (SQLite) or 'gcp' (Google Cloud Pub/Sub)"
    )

    args = parser.parse_args()
    worker_mode = args.worker_mode or ("experiment" if args.experiment_id else "normal")

    run_worker(
        worker_id=args.worker_id,
        poll_interval=args.poll_interval,
        exit_when_empty=args.exit_when_empty,
        experiment_id=args.experiment_id,
        mode=args.mode,
        worker_mode=worker_mode
    )
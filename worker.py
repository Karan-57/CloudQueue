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
    # Enable WAL mode and busy timeout for safe multi-process concurrency
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.isolation_level = None  # Explicit transaction control
    return conn


def claim_job(conn, worker_id, experiment_id=None):
    """
    Atomically claims ONE queued job using SQLite's BEGIN IMMEDIATE
    and UPDATE ... RETURNING.
    
    Used in 'local' SQLite mode.
    If experiment_id is provided, claims only from `experiment_jobs` matching that experiment.
    Otherwise, claims from the general `jobs` queue.
    """
    table = "experiment_jobs" if experiment_id else "jobs"
    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = time.time()
            cursor = conn.cursor()

            if experiment_id:
                cursor.execute(f"""
                    UPDATE {table}
                    SET status = 'Processing',
                        started_at = ?,
                        worker_id = ?
                    WHERE id = (
                        SELECT id FROM {table}
                        WHERE experiment_id = ? AND status = 'Queued'
                        ORDER BY id ASC
                        LIMIT 1
                    )
                    RETURNING id, job_name, workload_type, submitted_at
                """, (now, worker_id, experiment_id))
            else:
                cursor.execute(f"""
                    UPDATE {table}
                    SET status = 'Processing',
                        started_at = ?,
                        worker_id = ?
                    WHERE id = (
                        SELECT id FROM {table}
                        WHERE status = 'Queued'
                        ORDER BY id ASC
                        LIMIT 1
                    )
                    RETURNING id, job_name, workload_type, submitted_at
                """, (now, worker_id))

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
    """
    Atomically claims ONE specific job by ID in SQLite using BEGIN IMMEDIATE.
    Used in 'gcp' Pub/Sub mode where the worker receives a specific job ID from the queue.
    
    Returns:
      (status_code, job_tuple_or_none)
      status_code can be:
        'CLAIMED': (id, job_name, workload_type, submitted_at)
        'ALREADY_COMPLETED': Already completed in DB -> Safe to ACK without reprocessing
        'ALREADY_PROCESSING': Currently processed by another worker
        'NOT_FOUND': Record does not exist in DB -> Safe to ACK
    """
    table = "experiment_jobs" if experiment_id else "jobs"
    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            now = time.time()

            # First verify the job's current status in SQLite
            if experiment_id:
                cursor.execute(f"SELECT status, job_name, workload_type, submitted_at FROM {table} WHERE id = ? AND experiment_id = ?", (job_id, experiment_id))
            else:
                cursor.execute(f"SELECT status, job_name, workload_type, submitted_at FROM {table} WHERE id = ?", (job_id,))

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

            # Job is 'Queued', atomically claim it
            if experiment_id:
                cursor.execute(f"""
                    UPDATE {table}
                    SET status = 'Processing',
                        started_at = ?,
                        worker_id = ?
                    WHERE id = ? AND experiment_id = ? AND status = 'Queued'
                    RETURNING id, job_name, workload_type, submitted_at
                """, (now, worker_id, job_id, experiment_id))
            else:
                cursor.execute(f"""
                    UPDATE {table}
                    SET status = 'Processing',
                        started_at = ?,
                        worker_id = ?
                    WHERE id = ? AND status = 'Queued'
                    RETURNING id, job_name, workload_type, submitted_at
                """, (now, worker_id, job_id))

            claimed = cursor.fetchone()
            conn.execute("COMMIT")
            if claimed:
                return ("CLAIMED", claimed)
            else:
                return ("ALREADY_PROCESSING", None)

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
    """
    Simulates the requested workload without holding any database lock.
    """
    start_time = time.time()

    if workload_type == "CPU":
        total = 0
        for i in range(1, 1000000):
            total += i * i

    elif workload_type == "DATA":
        data = [i for i in range(100000)]
        data.sort()

    elif workload_type == "TEXT":
        text = "CloudQueue workload processing " * 10000
        text = text.lower()
        text = text.upper()

    else:
        time.sleep(2)

    end_time = time.time()
    return start_time, end_time


def complete_job(conn, job_id, submitted_at, started_at, completed_at, experiment_id=None):
    """
    Records completed status, processing time, waiting time, and total latency in SQLite.
    """
    table = "experiment_jobs" if experiment_id else "jobs"
    processing_time = round(completed_at - started_at, 3)
    waiting_time = round(started_at - submitted_at, 3) if submitted_at else 0.0
    total_latency = round(completed_at - submitted_at, 3) if submitted_at else processing_time

    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            cursor.execute(f"""
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


def run_gcp_worker(worker_id, poll_interval=0.5, exit_when_empty=False, experiment_id=None, worker_mode="normal"):
    """
    GCP Pub/Sub Worker execution loop.
    Multiple workers pull from the SAME shared Pub/Sub subscription.
    Atomically claims received job_id in SQLite, runs workload, updates DB, and ACKs message.
    """
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
            # Synchronous pull 1 message at a time to prevent hoarding among concurrent workers
            messages = pubsub_client.pull_messages(max_messages=1, timeout=2.0)

            if not messages:
                if exit_when_empty:
                    # Check SQLite if any queued jobs remain for this queue / experiment
                    cursor = conn.cursor()
                    table = "experiment_jobs" if experiment_id else "jobs"
                    if experiment_id:
                        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE experiment_id = ? AND status = 'Queued'", (experiment_id,))
                    else:
                        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE status = 'Queued'")
                    queued_count = cursor.fetchone()[0]

                    if queued_count == 0:
                        # Extra brief safety pause to confirm no in-flight burst
                        time.sleep(0.2)
                        if experiment_id:
                            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE experiment_id = ? AND status = 'Queued'", (experiment_id,))
                        else:
                            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE status = 'Queued'")
                        if cursor.fetchone()[0] == 0:
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

            # 1. Parse JSON payload
            try:
                if isinstance(raw_data, bytes):
                    payload_str = raw_data.decode("utf-8")
                else:
                    payload_str = str(raw_data)
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

            # 2. Worker filtering:
            # If this worker is bound to a specific experiment, but message is for another experiment or normal queue
            if experiment_id and msg_exp_id != experiment_id:
                pubsub_client.nack_message(ack_id)
                time.sleep(0.05)
                continue

            # If this worker is a standard worker, but message is for an experiment
            if not experiment_id and msg_exp_id:
                pubsub_client.nack_message(ack_id)
                time.sleep(0.05)
                continue

            # 3. Atomically claim specific job in SQLite
            status_code, job_data = claim_specific_job(conn, job_id, worker_id, experiment_id=experiment_id)

            if status_code == "ALREADY_COMPLETED":
                # Redelivered message for a job that finished earlier
                print(f"{context_label} Job #{job_id} already Completed in DB (redelivery). Safely ACKing message.")
                pubsub_client.acknowledge_message(ack_id)
                continue

            elif status_code == "ALREADY_PROCESSING":
                # Job currently being processed by another worker; NACK to let Pub/Sub handle retry if needed
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

            # 4. Job successfully claimed
            claimed_id, job_name, workload_type, submitted_at = job_data
            wait_time_preview = round(time.time() - submitted_at, 3) if submitted_at else 0.0
            print(f"{context_label} Claimed Job #{claimed_id} ({workload_type}) via Pub/Sub | Waiting: {wait_time_preview}s")

            # 5. Execute workload
            work_start, work_end = execute_workload(workload_type)

            # 6. Complete job in SQLite
            p_time, w_time, t_lat = complete_job(
                conn, claimed_id, submitted_at, work_start, work_end, experiment_id=experiment_id
            )

            # 7. ACK only AFTER successful SQLite completion write
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

    # LOCAL MODE: SQLite polling and atomic claiming
    conn = get_db()
    jobs_processed = 0
    empty_notified = False

    target_queue_name = f"experiment_jobs ({experiment_id})" if (worker_mode == "experiment" and experiment_id) else "jobs (standard)"
    print(f"{context_label} Target Queue: {target_queue_name}")

    try:
        while True:
            # If experiment mode, only claim matching experiment_id; otherwise claim normal jobs
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
                    # Brief verification to avoid premature exit during rapid burst inserts
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
        help="Exit automatically when no queued jobs remain (useful for benchmarks & experiments)"
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
        help="Queue mode: 'local' (SQLite) or 'gcp' (Google Cloud Pub/Sub, Phase 2)"
    )

    args = parser.parse_args()

    worker_mode = args.worker_mode
    if not worker_mode:
        worker_mode = "experiment" if args.experiment_id else "normal"

    run_worker(
        worker_id=args.worker_id,
        poll_interval=args.poll_interval,
        exit_when_empty=args.exit_when_empty,
        experiment_id=args.experiment_id,
        mode=args.mode,
        worker_mode=worker_mode
    )
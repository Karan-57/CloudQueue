from flask import Flask, request, jsonify, render_template
import sqlite3
import time
import os
import sys
import threading
import subprocess
import json
import config

app = Flask(__name__)

DB = config.DATABASE_PATH


def get_db():
    db_path = config.DATABASE_PATH
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    # Enable WAL mode and 30s busy timeout for concurrent multi-process safety
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def init_db():
    conn = get_db()
    cursor = conn.cursor()

    # Standard Jobs Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            workload_type TEXT NOT NULL,
            status TEXT NOT NULL,
            submitted_at REAL,
            started_at REAL,
            completed_at REAL,
            processing_time REAL,
            waiting_time REAL,
            total_latency REAL,
            worker_id TEXT
        )
    """)

    # Migrate existing jobs table if newer columns are not yet present
    cursor.execute("PRAGMA table_info(jobs)")
    existing_cols = {col[1] for col in cursor.fetchall()}

    if "waiting_time" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN waiting_time REAL")
    if "total_latency" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN total_latency REAL")
    if "worker_id" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN worker_id TEXT")

    # Experiments Table (Completely isolated from standard jobs)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS experiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id TEXT UNIQUE NOT NULL,
            worker_count INTEGER NOT NULL,
            job_count INTEGER NOT NULL,
            workload_type TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at REAL,
            completed_at REAL,
            total_time REAL,
            average_waiting_time REAL,
            average_processing_time REAL,
            average_total_latency REAL,
            throughput REAL,
            completed_jobs INTEGER DEFAULT 0,
            failed_jobs INTEGER DEFAULT 0,
            worker_distribution TEXT
        )
    """)

    # Experiment Jobs Table (Completely isolated from standard jobs table)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS experiment_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id TEXT NOT NULL,
            job_name TEXT NOT NULL,
            workload_type TEXT NOT NULL,
            status TEXT NOT NULL,
            submitted_at REAL,
            started_at REAL,
            completed_at REAL,
            processing_time REAL,
            waiting_time REAL,
            total_latency REAL,
            worker_id TEXT
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_exp_jobs ON experiment_jobs (experiment_id, status);")

    conn.commit()
    conn.close()


# ==============================================================================
# Standard Queue & Producer Endpoints
# ==============================================================================

@app.route("/")
def home():
    return render_template("index.html")


@app.route("/submit", methods=["POST"])
def submit_job():
    data = request.get_json() or {}

    job_name = data.get("job_name", "Test Job").strip()
    workload_type = data.get("workload_type", "CPU")

    now = time.time()
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO jobs
        (job_name, workload_type, status, submitted_at)
        VALUES (?, ?, ?, ?)
    """, (
        job_name,
        workload_type,
        "Queued",
        now
    ))

    conn.commit()
    job_id = cursor.lastrowid
    conn.close()

    # GCP Mode: Publish to Pub/Sub
    if config.is_gcp_mode():
        try:
            import pubsub_client
            pubsub_client.publish_job(job_id)
        except Exception as e:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("UPDATE jobs SET status = 'Failed' WHERE id = ?", (job_id,))
            conn.commit()
            conn.close()
            return jsonify({
                "error": f"Failed to publish job #{job_id} to Pub/Sub: {str(e)}",
                "job_id": job_id,
                "status": "Failed"
            }), 500

    return jsonify({
        "message": "Job submitted successfully",
        "job_id": job_id,
        "status": "Queued"
    })


@app.route("/submit_batch", methods=["POST"])
def submit_batch():
    data = request.get_json() or {}

    count = int(data.get("count", 100))
    workload_type = data.get("workload_type", "CPU")
    prefix = data.get("prefix", "Benchmark Job").strip()

    now = time.time()
    batch_records = [
        (f"{prefix} #{i+1}", workload_type, "Queued", now)
        for i in range(count)
    ]

    conn = get_db()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT INTO jobs
        (job_name, workload_type, status, submitted_at)
        VALUES (?, ?, ?, ?)
    """, batch_records)

    conn.commit()

    cursor.execute("""
        SELECT id FROM jobs ORDER BY id DESC LIMIT ?
    """, (count,))
    rows = cursor.fetchall()
    job_ids = [r[0] for r in reversed(rows)]
    first_id = job_ids[0] if job_ids else 1
    last_id = job_ids[-1] if job_ids else count
    conn.close()

    # GCP Mode: Publish batch to Pub/Sub
    if config.is_gcp_mode():
        try:
            import pubsub_client
            pubsub_client.publish_jobs_batch(job_ids)
        except Exception as e:
            conn = get_db()
            cursor = conn.cursor()
            cursor.executemany("UPDATE jobs SET status = 'Failed' WHERE id = ?", [(jid,) for jid in job_ids])
            conn.commit()
            conn.close()
            return jsonify({
                "error": f"Failed to publish batch jobs to Pub/Sub: {str(e)}",
                "status": "Failed"
            }), 500

    return jsonify({
        "message": f"Successfully queued {count} jobs",
        "count": count,
        "workload_type": workload_type,
        "first_job_id": first_id,
        "last_job_id": last_id,
        "status": "Queued"
    })


@app.route("/jobs")
def get_jobs():
    limit = request.args.get("limit", default=100, type=int)

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, job_name, workload_type, status,
               submitted_at, started_at, completed_at,
               processing_time, waiting_time, total_latency,
               worker_id
        FROM jobs
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))

    rows = cursor.fetchall()
    conn.close()

    jobs = []
    for row in rows:
        jobs.append({
            "id": row[0],
            "job_name": row[1],
            "workload_type": row[2],
            "status": row[3],
            "submitted_at": row[4],
            "started_at": row[5],
            "completed_at": row[6],
            "processing_time": row[7],
            "waiting_time": row[8],
            "total_latency": row[9],
            "worker_id": row[10]
        })

    return jsonify(jobs)


@app.route("/stats")
def stats():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM jobs")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM jobs WHERE status = 'Completed'")
    completed = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM jobs WHERE status = 'Queued'")
    queued = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM jobs WHERE status = 'Processing'")
    processing = cursor.fetchone()[0]

    cursor.execute("SELECT AVG(processing_time) FROM jobs WHERE processing_time IS NOT NULL")
    avg_proc = cursor.fetchone()[0]

    cursor.execute("SELECT AVG(waiting_time) FROM jobs WHERE waiting_time IS NOT NULL")
    avg_wait = cursor.fetchone()[0]

    cursor.execute("SELECT AVG(total_latency) FROM jobs WHERE total_latency IS NOT NULL")
    avg_lat = cursor.fetchone()[0]

    cursor.execute("""
        SELECT worker_id, COUNT(*)
        FROM jobs
        WHERE status = 'Completed' AND worker_id IS NOT NULL
        GROUP BY worker_id
        ORDER BY worker_id
    """)
    worker_rows = cursor.fetchall()
    workers = {row[0]: row[1] for row in worker_rows}

    conn.close()

    return jsonify({
        "total": total,
        "completed": completed,
        "queued": queued,
        "processing": processing,
        "avg_time": round(avg_proc, 3) if avg_proc else 0,
        "avg_processing_time": round(avg_proc, 3) if avg_proc else 0,
        "avg_waiting_time": round(avg_wait, 3) if avg_wait else 0,
        "avg_total_latency": round(avg_lat, 3) if avg_lat else 0,
        "workers": workers
    })


@app.route("/comparison")
def comparison():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT workload_type,
               COUNT(*),
               AVG(processing_time),
               AVG(waiting_time)
        FROM jobs
        WHERE status = 'Completed'
        GROUP BY workload_type
    """)

    rows = cursor.fetchall()
    conn.close()

    result = []
    for row in rows:
        result.append({
            "workload_type": row[0],
            "jobs": row[1],
            "average_time": round(row[2], 3) if row[2] else 0,
            "average_waiting_time": round(row[3], 3) if row[3] else 0
        })

    return jsonify(result)


@app.route("/reset", methods=["POST"])
def reset_queue():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM jobs")
    conn.commit()
    conn.close()
    return jsonify({"message": "All standard jobs and queue reset successfully"})


# ==============================================================================
# Isolated Experiment System (Multi-Worker Burst Scaling Studies)
# ==============================================================================

def _run_experiment_orchestrator(experiment_id, worker_count):
    """
    Background orchestrator thread.
    Spawns exactly `worker_count` independent worker processes targeting
    `experiment_jobs` for this specific `experiment_id`.
    When workers terminate, aggregates metrics and updates the experiment record.
    """
    processes = []
    for i in range(1, worker_count + 1):
        worker_id = f"exp-{experiment_id}-w{i}"
        cmd = [
            sys.executable,
            "worker.py",
            "--experiment-id", experiment_id,
            "--worker-id", worker_id,
            "--mode", config.CLOUDQUEUE_MODE,
            "--exit-when-empty",
            "--poll-interval", "0.1"
        ]
        p = subprocess.Popen(cmd, env=os.environ.copy())
        processes.append(p)

    for p in processes:
        p.wait()

    # Finalize experiment metrics
    now = time.time()
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT started_at, job_count FROM experiments WHERE experiment_id = ?", (experiment_id,))
    exp_meta = cursor.fetchone()
    started_at = exp_meta[0] if exp_meta else now
    job_count = exp_meta[1] if exp_meta else 0

    total_time = round(now - started_at, 3)

    cursor.execute("SELECT COUNT(*) FROM experiment_jobs WHERE experiment_id = ? AND status = 'Completed'", (experiment_id,))
    completed_jobs = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM experiment_jobs WHERE experiment_id = ? AND status = 'Failed'", (experiment_id,))
    failed_jobs = cursor.fetchone()[0]

    cursor.execute("SELECT AVG(waiting_time) FROM experiment_jobs WHERE experiment_id = ? AND waiting_time IS NOT NULL", (experiment_id,))
    avg_wait = cursor.fetchone()[0] or 0.0

    cursor.execute("SELECT AVG(processing_time) FROM experiment_jobs WHERE experiment_id = ? AND processing_time IS NOT NULL", (experiment_id,))
    avg_proc = cursor.fetchone()[0] or 0.0

    cursor.execute("SELECT AVG(total_latency) FROM experiment_jobs WHERE experiment_id = ? AND total_latency IS NOT NULL", (experiment_id,))
    avg_lat = cursor.fetchone()[0] or 0.0

    throughput = round(completed_jobs / total_time, 2) if total_time > 0 else 0.0

    cursor.execute("""
        SELECT worker_id, COUNT(*)
        FROM experiment_jobs
        WHERE experiment_id = ? AND status = 'Completed' AND worker_id IS NOT NULL
        GROUP BY worker_id
        ORDER BY worker_id
    """, (experiment_id,))
    w_rows = cursor.fetchall()
    worker_distribution = {r[0]: r[1] for r in w_rows}

    cursor.execute("""
        UPDATE experiments
        SET status = 'Completed',
            completed_at = ?,
            total_time = ?,
            completed_jobs = ?,
            failed_jobs = ?,
            average_waiting_time = ?,
            average_processing_time = ?,
            average_total_latency = ?,
            throughput = ?,
            worker_distribution = ?
        WHERE experiment_id = ?
    """, (
        now,
        total_time,
        completed_jobs,
        failed_jobs,
        round(avg_wait, 3),
        round(avg_proc, 3),
        round(avg_lat, 3),
        throughput,
        json.dumps(worker_distribution),
        experiment_id
    ))

    conn.commit()
    conn.close()


@app.route("/experiments/run", methods=["POST"])
def run_experiment():
    data = request.get_json() or {}

    worker_count = int(data.get("worker_count", 2))
    job_count = int(data.get("job_count", 50))
    workload_type = data.get("workload_type", "CPU")

    if worker_count not in [1, 2, 4, 8]:
        worker_count = 2

    if job_count < 1:
        job_count = 10

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM experiments")
    count_now = cursor.fetchone()[0]
    experiment_id = f"EXP-{(count_now + 1):03d}"

    now = time.time()

    cursor.execute("""
        INSERT INTO experiments
        (experiment_id, worker_count, job_count, workload_type, status, started_at)
        VALUES (?, ?, ?, ?, 'Running', ?)
    """, (experiment_id, worker_count, job_count, workload_type, now))

    jobs_data = [
        (experiment_id, f"{experiment_id} #{i+1}", workload_type, "Queued", now)
        for i in range(job_count)
    ]

    cursor.executemany("""
        INSERT INTO experiment_jobs
        (experiment_id, job_name, workload_type, status, submitted_at)
        VALUES (?, ?, ?, ?, ?)
    """, jobs_data)

    conn.commit()

    cursor.execute("""
        SELECT id FROM experiment_jobs WHERE experiment_id = ? ORDER BY id ASC
    """, (experiment_id,))
    exp_job_ids = [r[0] for r in cursor.fetchall()]
    conn.close()

    # GCP Mode: Publish experiment jobs to Pub/Sub
    if config.is_gcp_mode():
        try:
            import pubsub_client
            pubsub_client.publish_jobs_batch(exp_job_ids, experiment_id=experiment_id)
        except Exception as e:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("UPDATE experiments SET status = 'Failed' WHERE experiment_id = ?", (experiment_id,))
            cursor.execute("UPDATE experiment_jobs SET status = 'Failed' WHERE experiment_id = ?", (experiment_id,))
            conn.commit()
            conn.close()
            return jsonify({
                "error": f"Failed to publish experiment jobs to Pub/Sub: {str(e)}",
                "status": "Failed"
            }), 500

    # Launch worker processes via background orchestrator thread (Flask never blocks)
    orch_thread = threading.Thread(
        target=_run_experiment_orchestrator,
        args=(experiment_id, worker_count),
        daemon=True
    )
    orch_thread.start()

    return jsonify({
        "message": "Experiment started successfully",
        "experiment_id": experiment_id,
        "worker_count": worker_count,
        "job_count": job_count,
        "workload_type": workload_type,
        "status": "Running"
    })


@app.route("/experiments", methods=["GET"])
def list_experiments():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, experiment_id, worker_count, job_count, workload_type,
               status, started_at, completed_at, total_time,
               average_waiting_time, average_processing_time,
               average_total_latency, throughput, completed_jobs, failed_jobs,
               worker_distribution
        FROM experiments
        ORDER BY id DESC
    """)

    rows = cursor.fetchall()
    conn.close()

    experiments = []
    for r in rows:
        w_dist = {}
        if r[15]:
            try:
                w_dist = json.loads(r[15])
            except Exception:
                pass

        experiments.append({
            "id": r[0],
            "experiment_id": r[1],
            "worker_count": r[2],
            "job_count": r[3],
            "workload_type": r[4],
            "status": r[5],
            "started_at": r[6],
            "completed_at": r[7],
            "total_time": r[8],
            "average_waiting_time": r[9],
            "average_processing_time": r[10],
            "average_total_latency": r[11],
            "throughput": r[12],
            "completed_jobs": r[13],
            "failed_jobs": r[14],
            "worker_distribution": w_dist
        })

    return jsonify(experiments)


@app.route("/experiments/<experiment_id>", methods=["GET"])
def get_experiment_details(experiment_id):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, experiment_id, worker_count, job_count, workload_type,
               status, started_at, completed_at, total_time,
               average_waiting_time, average_processing_time,
               average_total_latency, throughput, completed_jobs, failed_jobs,
               worker_distribution
        FROM experiments
        WHERE experiment_id = ?
    """, (experiment_id,))

    exp = cursor.fetchone()
    if not exp:
        conn.close()
        return jsonify({"error": "Experiment not found"}), 404

    # Live progress query
    cursor.execute("SELECT COUNT(*) FROM experiment_jobs WHERE experiment_id = ? AND status = 'Completed'", (experiment_id,))
    completed = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM experiment_jobs WHERE experiment_id = ? AND status = 'Queued'", (experiment_id,))
    queued = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM experiment_jobs WHERE experiment_id = ? AND status = 'Processing'", (experiment_id,))
    processing = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM experiment_jobs WHERE experiment_id = ? AND status = 'Failed'", (experiment_id,))
    failed = cursor.fetchone()[0]

    conn.close()

    total = exp[3]
    started_at = exp[6]
    completed_at = exp[7]
    elapsed = round(time.time() - started_at, 1) if (exp[5] == "Running" and started_at) else exp[8]

    w_dist = {}
    if exp[15]:
        try:
            w_dist = json.loads(exp[15])
        except Exception:
            pass

    return jsonify({
        "experiment_id": exp[1],
        "worker_count": exp[2],
        "job_count": total,
        "workload_type": exp[4],
        "status": exp[5],
        "started_at": started_at,
        "completed_at": completed_at,
        "total_time": exp[8],
        "elapsed_time": elapsed,
        "progress": {
            "completed": completed,
            "queued": queued,
            "processing": processing,
            "failed": failed,
            "percentage": round((completed / total * 100), 1) if total > 0 else 0
        },
        "metrics": {
            "total_jobs": total,
            "completed_jobs": completed,
            "failed_jobs": failed,
            "total_time": exp[8],
            "average_waiting_time": exp[9],
            "average_processing_time": exp[10],
            "average_total_latency": exp[11],
            "throughput": exp[12],
            "worker_distribution": w_dist
        }
    })


@app.route("/experiments/<experiment_id>/jobs", methods=["GET"])
def get_experiment_jobs(experiment_id):
    limit = request.args.get("limit", default=50, type=int)

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, job_name, workload_type, status,
               submitted_at, started_at, completed_at,
               processing_time, waiting_time, total_latency,
               worker_id
        FROM experiment_jobs
        WHERE experiment_id = ?
        ORDER BY id DESC
        LIMIT ?
    """, (experiment_id, limit))

    rows = cursor.fetchall()
    conn.close()

    jobs = []
    for row in rows:
        jobs.append({
            "id": row[0],
            "job_name": row[1],
            "workload_type": row[2],
            "status": row[3],
            "submitted_at": row[4],
            "started_at": row[5],
            "completed_at": row[6],
            "processing_time": row[7],
            "waiting_time": row[8],
            "total_latency": row[9],
            "worker_id": row[10]
        })

    return jsonify(jobs)


@app.route("/experiments/compare", methods=["POST"])
def compare_experiments():
    data = request.get_json() or {}
    exp_ids = data.get("experiment_ids", [])

    if not isinstance(exp_ids, list) or len(exp_ids) < 2:
        return jsonify({
            "valid": False,
            "error": "At least two experiments must be selected for comparison."
        }), 400

    conn = get_db()
    cursor = conn.cursor()

    placeholders = ",".join(["?"] * len(exp_ids))
    cursor.execute(f"""
        SELECT experiment_id, worker_count, job_count, workload_type,
               status, total_time, throughput, average_waiting_time,
               average_processing_time, average_total_latency,
               completed_jobs, failed_jobs
        FROM experiments
        WHERE experiment_id IN ({placeholders})
    """, exp_ids)

    rows = cursor.fetchall()
    conn.close()

    if len(rows) < len(exp_ids):
        return jsonify({
            "valid": False,
            "error": "One or more selected experiments could not be found."
        }), 404

    # Check statuses: all must be Completed
    not_completed = [r[0] for r in rows if r[4] != "Completed"]
    if not_completed:
        return jsonify({
            "valid": False,
            "error": f"All selected experiments must be Completed. Pending: {', '.join(not_completed)}."
        }), 400

    # Collect workloads and job counts
    workloads = set(r[3] for r in rows)
    job_counts = set(r[2] for r in rows)

    if len(workloads) > 1 or len(job_counts) > 1:
        workload_str = ", ".join(sorted(workloads))
        job_count_str = ", ".join(str(j) for j in sorted(job_counts))
        return jsonify({
            "valid": False,
            "error": f"Selected experiments cannot be directly compared because their workload or job count differs. "
                     f"(Workloads: {workload_str} | Job counts: {job_count_str}). "
                     f"To isolate the effect of worker scaling, both the workload and job count must be identical."
        }), 400

    # Sort by worker_count ASC
    sorted_rows = sorted(rows, key=lambda r: r[1])

    workload = sorted_rows[0][3]
    job_count = sorted_rows[0][2]

    experiments_data = []
    for r in sorted_rows:
        experiments_data.append({
            "experiment_id": r[0],
            "worker_count": r[1],
            "job_count": r[2],
            "workload_type": r[3],
            "total_time": r[5],
            "throughput": r[6],
            "average_waiting_time": r[7],
            "average_processing_time": r[8],
            "average_total_latency": r[9],
            "completed_jobs": r[10],
            "failed_jobs": r[11]
        })

    return jsonify({
        "valid": True,
        "workload_type": workload,
        "job_count": job_count,
        "experiments": experiments_data
    })


if __name__ == "__main__":
    init_db()

    port = config.PORT

    app.run(
        host="0.0.0.0",
        port=port,
        debug=True
    )
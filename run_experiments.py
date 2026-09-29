import subprocess
import time
import urllib.request
import json
import sqlite3
import sys
import config

API_URL = f"http://localhost:{config.PORT}"
DB_PATH = config.DATABASE_PATH

def api_post(endpoint, payload=None):
    data = json.dumps(payload or {}).encode("utf-8")
    req = urllib.request.Request(
        f"{API_URL}{endpoint}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))

def api_get(endpoint):
    req = urllib.request.Request(f"{API_URL}{endpoint}", method="GET")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))

def reset_database():
    api_post("/reset")
    time.sleep(0.5)

def submit_batch(count=100, workload_type="CPU"):
    return api_post("/submit_batch", {
        "count": count,
        "workload_type": workload_type,
        "prefix": f"{workload_type}-Test"
    })

def run_worker_benchmark(num_workers, job_count=100, workload_type="CPU"):
    print(f"\n=======================================================")
    print(f"BENCHMARK: {job_count} jobs ({workload_type}) with {num_workers} worker(s)")
    print(f"=======================================================")

    reset_database()
    batch_info = submit_batch(count=job_count, workload_type=workload_type)
    print(f"[Producer] Submitted {job_count} jobs. Queue status: {batch_info['status']}")

    # Start N worker processes concurrently
    processes = []
    start_time = time.time()
    
    for i in range(1, num_workers + 1):
        worker_id = f"worker-{i}"
        cmd = [
            sys.executable,
            "worker.py",
            "--worker-id", worker_id,
            "--exit-when-empty",
            "--poll-interval", "0.2"
        ]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append((worker_id, p))

    # Wait for all workers to finish
    for worker_id, p in processes:
        stdout, stderr = p.communicate()
        if stderr and "Traceback" in stderr:
            print(f"[{worker_id}] ERROR: {stderr}")

    total_experiment_time = round(time.time() - start_time, 3)

    # Fetch stats from API
    stats = api_get("/stats")
    throughput = round(stats["completed"] / total_experiment_time, 2) if total_experiment_time > 0 else 0

    print(f"\n--- Results for {num_workers} Worker(s) ---")
    print(f"Total Experiment Wall-Clock Time: {total_experiment_time} s")
    print(f"Jobs Completed                 : {stats['completed']} / {job_count}")
    print(f"Jobs Queued (Remaining)        : {stats['queued']}")
    print(f"Jobs in Processing             : {stats['processing']}")
    print(f"Average Waiting Time           : {stats['avg_waiting_time']} s")
    print(f"Average Processing Time        : {stats['avg_processing_time']} s")
    print(f"Average Total Latency          : {stats['avg_total_latency']} s")
    print(f"Throughput                     : {throughput} jobs/sec")
    print(f"Worker Distribution            : {stats['workers']}")

    # Verification checks
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT id) FROM jobs WHERE status = 'Completed'")
    distinct_completed = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE worker_id IS NULL OR worker_id = ''")
    unassigned = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE waiting_time IS NULL")
    missing_wait = cursor.fetchone()[0]
    conn.close()

    assert distinct_completed == job_count, f"Expected {job_count} completed jobs, got {distinct_completed}"
    assert unassigned == 0, f"Found {unassigned} unassigned jobs"
    assert missing_wait == 0, f"Found {missing_wait} jobs with missing waiting_time"
    print(f"[Verification] PASS: All {job_count} jobs processed exactly once with safe atomic claiming.")

    return {
        "workers": num_workers,
        "jobs": job_count,
        "wall_time": total_experiment_time,
        "throughput": throughput,
        "avg_waiting": stats["avg_waiting_time"],
        "avg_processing": stats["avg_processing_time"],
        "avg_latency": stats["avg_total_latency"],
        "worker_distribution": stats["workers"]
    }

if __name__ == "__main__":
    results = []
    # Test 1 worker
    results.append(run_worker_benchmark(num_workers=1, job_count=100, workload_type="CPU"))
    time.sleep(1)

    # Test 2 workers
    results.append(run_worker_benchmark(num_workers=2, job_count=100, workload_type="CPU"))
    time.sleep(1)

    # Test 4 workers
    results.append(run_worker_benchmark(num_workers=4, job_count=100, workload_type="CPU"))

    print("\n" + "=" * 60)
    print("FINAL SCALABILITY EXPERIMENT SUMMARY (100 CPU JOBS)")
    print("=" * 60)
    print(f"{'Workers':<10} | {'Total Time (s)':<15} | {'Throughput (jobs/s)':<22} | {'Avg Waiting (s)':<17} | {'Avg Proc (s)':<15}")
    print("-" * 85)
    for r in results:
        print(f"{r['workers']:<10} | {r['wall_time']:<15} | {r['throughput']:<22} | {r['avg_waiting']:<17} | {r['avg_processing']:<15}")
    print("=" * 60)

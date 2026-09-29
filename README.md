# CloudQueue

CloudQueue is an asynchronous queue-processing and performance experimentation platform. It allows users to study how queue architectures behave under sudden traffic spikes and experimentally measure the throughput, waiting time, processing latency, and scalability impact of increasing concurrent workers (1, 2, 4, 8 workers).

---

## Architecture Overview

CloudQueue supports two interchangeable queue modes:

1. **Local Mode (`CLOUDQUEUE_MODE=local`)**:
   - **Producer**: Flask web application.
   - **Queue**: Embedded SQLite database (`database/cloudqueue.db`) using atomic `BEGIN IMMEDIATE` / `UPDATE ... RETURNING` transactions.
   - **Workers**: Detached Python worker processes (`worker.py`).
   - **Dashboard**: Real-time monitoring, experiment history, and comparative performance analysis.

2. **GCP Mode (`CLOUDQUEUE_MODE=gcp`)**:
   - **Producer**: Flask web application creates authoritative SQLite job records and publishes job IDs to Google Cloud Pub/Sub.
   - **Queue**: Google Cloud Pub/Sub topic (`cloudqueue-jobs`) and shared subscription (`cloudqueue-worker-sub`).
   - **Workers**: Independent background worker processes consuming from the shared subscription.
   - **Authoritative Database**: Standalone SQLite database with automated GitHub persistence.

---

## Database Architecture & Roles

- **`database/cloudqueue.db` (Local Working Database)**:
  The active working SQLite database used directly by the running Flask application and worker processes for real-time reads and writes.

- **`backups/cloudqueue.db` (Persisted GCP Result Database)**:
  The authoritative result snapshot created by the GCP automated backup system (`./gcp/backup_db.sh`) and committed to GitHub. Because Skills Boost GCP VMs are temporary, this database persists experimental results across sessions.

---

## Syncing GCP Results to Local

When you finish running experiments on a temporary GCP VM, sync the experimental data back to your local machine:

1. **Pull the latest backup from GitHub**:
   ```bash
   git pull
   ```

2. **Restore the persistent database into your local working database**:
   ```bash
   ./scripts/restore_results.sh
   ```
   *Note: This script automatically validates backup integrity, ensures no active CloudQueue processes are writing to the database, creates a timestamped safety backup of your existing local database in `.gcp/local_db_backups/`, and safely restores the database.*

3. **Start the local CloudQueue application**:
   ```bash
   python app.py
   ```

4. **View your restored data**:
   Open your browser to `http://localhost:5000` and navigate to **Experiment History** and **Experiment Comparison** to analyze your GCP benchmark results locally.

---

## Local Development Quickstart

1. **Create and activate a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Start the web application**:
   ```bash
   python app.py
   ```

4. **Start background workers (Local Mode)**:
   ```bash
   python worker.py --worker-id worker-1
   ```

---

## GCP Skills Boost Automated Setup (Phases 3–5)

- **Automated VM Bootstrap**:
  ```bash
  ./gcp/setup.sh
  ```
- **Automated Worker Process Management**:
  ```bash
  ./gcp/start_workers.sh 4    # Start 1, 2, 4, or 8 background workers
  ./gcp/stop_workers.sh       # Gracefully stop all background workers
  ```
- **Automated Database Persistence**:
  ```bash
  ./gcp/backup_db.sh              # Run immediate backup & push to GitHub
  ./gcp/backup_db.sh --install-cron  # Install 5-minute automated backup cron
  ```

---

## Real-GCP End-to-End Validation & Demonstration (Phase 7)

On your Google Cloud Skills Boost VM:

```bash
# Execute the full Phase 7 benchmark suite
./gcp/run_phase7.sh
```

This single command automatically executes:
1. **Environment Verification (Phase 7A)**: Detects active GCP project, verifies Python, Google Cloud Pub/Sub, SQLite tables, and `.env` configuration.
2. **Real Pub/Sub Smoke Test (Phase 7B)**: Verifies end-to-end publish, worker claim via atomic `BEGIN IMMEDIATE`, execution, SQLite completion, and message ACK.
3. **Controlled Performance Experiments (Phase 7C)**: Runs four controlled experiments on 100 CPU jobs across 1, 2, 4, and 8 independent worker processes, collecting wall-clock time, throughput, waiting time, processing latency, and worker distribution.
4. **Experiment Comparison (Phase 7D)**: Validates comparative table and charts (Total Time, Throughput, Avg Waiting Time vs Workers).
5. **Multi-Worker Shared Subscription Verification (Phase 7E)**: Demonstrates competing consumer message distribution across 4 concurrent workers pulling from the single shared `cloudqueue-worker-sub` subscription.
6. **Two-VM Demonstration (Phase 7F)**: Documents multi-worker single-VM vs multi-VM topology within Skills Boost quota limits.
7. **Persistence (Phase 7G)**: Backs up the populated SQLite database and pushes to GitHub.
8. **Final Validation Report (Phase 7I)**: Generates and prints a complete summary, saved to `.gcp/phase7_validation_report.md`.


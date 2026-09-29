"""
Phase 2 Verification Test Suite: GCP Pub/Sub Integration & Local Mode Regression
Tests:
1. Pub/Sub message structure and SQLite ID matching
2. Atomic claiming by specific ID (claim_specific_job)
3. Redelivery / deduplication safety (ACK without re-execution)
4. Malformed message handling
5. Multi-worker concurrency on shared subscription
6. Experiment execution and isolation in GCP mode
7. Full regression testing of Local mode (single, batch, multi-worker, experiments, comparison)
"""

import json
import os
import sqlite3
import sys
import time
import unittest

# Ensure project root is on sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

import config
import pubsub_client
from pubsub_client import MockPubSubBroker
import worker
from app import app, init_db, get_db


class TestPhase2PubSub(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.orig_env = {
            "DATABASE_PATH": os.environ.get("DATABASE_PATH"),
            "CLOUDQUEUE_MODE": os.environ.get("CLOUDQUEUE_MODE"),
            "CLOUDQUEUE_MOCK_PUBSUB": os.environ.get("CLOUDQUEUE_MOCK_PUBSUB"),
            "GOOGLE_CLOUD_PROJECT": os.environ.get("GOOGLE_CLOUD_PROJECT"),
            "PUBSUB_TOPIC": os.environ.get("PUBSUB_TOPIC"),
            "PUBSUB_SUBSCRIPTION": os.environ.get("PUBSUB_SUBSCRIPTION"),
        }
        cls.orig_db_path = config.DATABASE_PATH
        cls.orig_mode = config.CLOUDQUEUE_MODE
        cls.orig_project = config.GOOGLE_CLOUD_PROJECT
        cls.orig_topic = config.PUBSUB_TOPIC
        cls.orig_sub = config.PUBSUB_SUBSCRIPTION

        # Configure test database
        cls.test_db = os.path.join(BASE_DIR, "database", "test_phase2.db")
        os.environ["DATABASE_PATH"] = cls.test_db
        config.DATABASE_PATH = cls.test_db
        worker.DB = cls.test_db

        # Configure GCP mode with mock broker for deterministic testing
        os.environ["CLOUDQUEUE_MODE"] = "gcp"
        os.environ["CLOUDQUEUE_MOCK_PUBSUB"] = "true"
        os.environ["GOOGLE_CLOUD_PROJECT"] = "test-cloudqueue-project"
        os.environ["PUBSUB_TOPIC"] = "cloudqueue-jobs"
        os.environ["PUBSUB_SUBSCRIPTION"] = "cloudqueue-worker-sub"

        config.CLOUDQUEUE_MODE = "gcp"
        config.GOOGLE_CLOUD_PROJECT = "test-cloudqueue-project"
        config.PUBSUB_TOPIC = "cloudqueue-jobs"
        config.PUBSUB_SUBSCRIPTION = "cloudqueue-worker-sub"

        app.config["TESTING"] = True
        cls.client = app.test_client()

    @classmethod
    def tearDownClass(cls):
        for k, v in cls.orig_env.items():
            if v is not None:
                os.environ[k] = v
            else:
                os.environ.pop(k, None)
        config.DATABASE_PATH = cls.orig_db_path
        config.CLOUDQUEUE_MODE = cls.orig_mode
        config.GOOGLE_CLOUD_PROJECT = cls.orig_project
        config.PUBSUB_TOPIC = cls.orig_topic
        config.PUBSUB_SUBSCRIPTION = cls.orig_sub
        worker.DB = cls.orig_db_path
        if os.path.exists(cls.test_db):
            try:
                os.remove(cls.test_db)
            except OSError:
                pass

    def setUp(self):
        # Fresh database tables for each test
        init_db()
        conn = get_db()
        conn.execute("DELETE FROM jobs;")
        conn.execute("DELETE FROM experiment_jobs;")
        conn.execute("DELETE FROM experiments;")
        conn.commit()
        conn.close()

        # Reset mock broker
        self.mock_broker = MockPubSubBroker(clear_db=True)
        pubsub_client.set_clients(publisher=self.mock_broker, subscriber=self.mock_broker)

    # -------------------------------------------------------------------------
    # 1. Pub/Sub Message Payload Format & SQLite ID Verification
    # -------------------------------------------------------------------------
    def test_01_message_structure_normal_job(self):
        """Verify normal job Pub/Sub message contains EXACT existing SQLite job ID."""
        # 1. Submit job via Flask in GCP mode
        res = self.client.post("/submit", json={"job_name": "Test Payloads", "workload_type": "CPU"})
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        job_id = data["job_id"]
        self.assertTrue(job_id > 0)

        # 2. Check Pub/Sub broker received exactly 1 message
        self.assertEqual(len(self.mock_broker.queue), 1)
        raw_msg = self.mock_broker.queue[0]
        payload = json.loads(raw_msg.data.decode("utf-8"))

        # 3. Verify payload structure: MUST contain existing SQLite job_id
        self.assertEqual(payload, {"job_id": job_id})
        self.assertNotIn("workload_type", payload, "Payload must NOT duplicate workload data")
        self.assertNotIn("experiment_id", payload, "Normal job must NOT have experiment_id")

        # 4. Verify SQLite database contains the matching job record in 'Queued' status
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, job_name, workload_type, status FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], job_id)
        self.assertEqual(row[1], "Test Payloads")
        self.assertEqual(row[2], "CPU")
        self.assertEqual(row[3], "Queued")

    def test_02_message_structure_experiment_job(self):
        """Verify experiment job Pub/Sub message contains job_id and experiment_id."""
        res = self.client.post("/experiments/run", json={"worker_count": 2, "job_count": 5, "workload_type": "DATA"})
        self.assertEqual(res.status_code, 200)
        exp_id = res.get_json()["experiment_id"]

        # Check Pub/Sub queue has 5 messages
        self.assertEqual(len(self.mock_broker.queue), 5)
        for msg in self.mock_broker.queue:
            payload = json.loads(msg.data.decode("utf-8"))
            self.assertIn("job_id", payload)
            self.assertEqual(payload.get("experiment_id"), exp_id)
            self.assertNotIn("workload_type", payload)

    # -------------------------------------------------------------------------
    # 2. Atomic Specific Job Claiming (claim_specific_job)
    # -------------------------------------------------------------------------
    def test_03_atomic_claiming_by_specific_id(self):
        """Test claim_specific_job correctly claims only Queued jobs and prevents double claims."""
        conn = get_db()
        cursor = conn.cursor()
        now = time.time()
        cursor.execute("INSERT INTO jobs (job_name, workload_type, status, submitted_at) VALUES (?, ?, ?, ?)",
                       ("Job 1", "CPU", "Queued", now))
        job_id = cursor.lastrowid
        conn.commit()

        # Worker 1 claims job
        status1, job_data1 = worker.claim_specific_job(conn, job_id, "worker-1")
        self.assertEqual(status1, "CLAIMED")
        self.assertEqual(job_data1[0], job_id)
        self.assertEqual(job_data1[1], "Job 1")

        # Worker 2 attempts to claim the same job (currently Processing)
        status2, job_data2 = worker.claim_specific_job(conn, job_id, "worker-2")
        self.assertEqual(status2, "ALREADY_PROCESSING")
        self.assertIsNone(job_data2)

        # Worker 1 completes the job
        worker.complete_job(conn, job_id, now, now + 0.1, now + 0.2)

        # Worker 3 attempts to claim the job (now Completed)
        status3, job_data3 = worker.claim_specific_job(conn, job_id, "worker-3")
        self.assertEqual(status3, "ALREADY_COMPLETED")
        self.assertIsNone(job_data3)

        # Nonexistent job ID
        status_none, _ = worker.claim_specific_job(conn, 99999, "worker-1")
        self.assertEqual(status_none, "NOT_FOUND")
        conn.close()

    # -------------------------------------------------------------------------
    # 3. Redelivery / Deduplication Safety
    # -------------------------------------------------------------------------
    def test_04_redelivery_safety_and_ack(self):
        """Test that if a completed job's message is redelivered, worker ACKs without reprocessing."""
        conn = get_db()
        cursor = conn.cursor()
        now = time.time()
        cursor.execute("INSERT INTO jobs (job_name, workload_type, status, submitted_at, started_at, completed_at, processing_time) "
                       "VALUES (?, ?, 'Completed', ?, ?, ?, ?)",
                       ("Old Job", "TEXT", now - 10, now - 5, now - 4, 1.0))
        job_id = cursor.lastrowid
        conn.commit()
        conn.close()

        # Put a redelivered message into Pub/Sub queue
        pubsub_client.publish_job(job_id)
        self.assertEqual(len(self.mock_broker.queue), 1)

        # Run worker with exit_when_empty
        worker.run_gcp_worker("worker-redelivery", exit_when_empty=True)

        # Verify message was ACKed (queue is empty and unacked is empty)
        self.assertEqual(len(self.mock_broker.queue), 0)
        self.assertEqual(len(self.mock_broker.unacked), 0)

        # Verify the job remains Completed and was not re-processed
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status, processing_time FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        conn.close()
        self.assertEqual(row[0], "Completed")
        self.assertEqual(row[1], 1.0)

    # -------------------------------------------------------------------------
    # 4. Malformed Message Safety
    # -------------------------------------------------------------------------
    def test_05_malformed_messages_handled_cleanly(self):
        """Verify invalid or non-JSON messages are safely discarded/ACKed without crashing worker."""
        # 1. Non-JSON string
        self.mock_broker.publish("cloudqueue-jobs", b"NOT_A_VALID_JSON_STRING")
        # 2. JSON without job_id
        self.mock_broker.publish("cloudqueue-jobs", json.dumps({"something_else": 123}).encode("utf-8"))
        self.assertEqual(len(self.mock_broker.queue), 2)

        # Run worker
        worker.run_gcp_worker("worker-malformed", exit_when_empty=True)

        # Both malformed messages should be safely ACKed
        self.assertEqual(len(self.mock_broker.queue), 0)
        self.assertEqual(len(self.mock_broker.unacked), 0)

    # -------------------------------------------------------------------------
    # 5. Multi-Worker Concurrent Processing from Shared Subscription
    # -------------------------------------------------------------------------
    def test_06_multi_worker_shared_subscription(self):
        """Verify multiple workers pulling from the same subscription process all jobs without collision."""
        conn = get_db()
        cursor = conn.cursor()
        job_count = 12
        now = time.time()
        job_ids = []
        for i in range(job_count):
            cursor.execute("INSERT INTO jobs (job_name, workload_type, status, submitted_at) VALUES (?, ?, 'Queued', ?)",
                           (f"Shared Job #{i+1}", "CPU", now))
            job_ids.append(cursor.lastrowid)
        conn.commit()
        conn.close()

        # Publish all 12 job IDs
        pubsub_client.publish_jobs_batch(job_ids)
        self.assertEqual(len(self.mock_broker.queue), job_count)

        # Run 3 workers concurrently in threads or sequential interleaved pulls
        # To simulate multi-threaded concurrency:
        import threading
        threads = []
        for w_idx in range(3):
            t = threading.Thread(
                target=worker.run_gcp_worker,
                args=(f"worker-{w_idx+1}",),
                kwargs={"exit_when_empty": True}
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=15.0)

        # Verify all messages were ACKed
        self.assertEqual(len(self.mock_broker.queue), 0)
        self.assertEqual(len(self.mock_broker.unacked), 0)

        # Verify all 12 jobs are marked 'Completed' in SQLite
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status, worker_id FROM jobs WHERE id IN ({})".format(",".join("?" * job_count)), job_ids)
        rows = cursor.fetchall()
        conn.close()

        self.assertEqual(len(rows), job_count)
        workers_seen = set()
        for r in rows:
            self.assertEqual(r[0], "Completed")
            self.assertIsNotNone(r[1])
            workers_seen.add(r[1])

        # Multiple workers should have claimed jobs
        self.assertTrue(len(workers_seen) > 1, f"Expected multiple workers, saw: {workers_seen}")

    # -------------------------------------------------------------------------
    # 6. Local Mode Regression Verification
    # -------------------------------------------------------------------------
    def test_07_local_mode_full_regression(self):
        """Verify that CLOUDQUEUE_MODE=local continues to function with 100% fidelity."""
        # Temporarily switch mode to local
        os.environ["CLOUDQUEUE_MODE"] = "local"
        config.CLOUDQUEUE_MODE = "local"

        try:
            # 1. Normal submission
            res = self.client.post("/submit", json={"job_name": "Local Normal Job", "workload_type": "CPU"})
            self.assertEqual(res.status_code, 200)
            data = res.get_json()
            job_id = data["job_id"]
            self.assertEqual(data["status"], "Queued")

            # In local mode, Pub/Sub mock broker must receive 0 messages!
            self.assertEqual(len(self.mock_broker.queue), 0)

            # 2. Local worker processes job
            worker.run_worker("local-w1", exit_when_empty=True, mode="local")

            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT status, worker_id, processing_time FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            conn.close()

            self.assertEqual(row[0], "Completed")
            self.assertEqual(row[1], "local-w1")
            self.assertIsNotNone(row[2])

            # 3. Batch submission
            batch_res = self.client.post("/submit_batch", json={"count": 5, "workload_type": "DATA"})
            self.assertEqual(batch_res.status_code, 200)
            worker.run_worker("local-w2", exit_when_empty=True, mode="local")

            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM jobs WHERE status = 'Completed'")
            completed_total = cursor.fetchone()[0]
            conn.close()
            self.assertEqual(completed_total, 6)

        finally:
            # Restore GCP mode
            os.environ["CLOUDQUEUE_MODE"] = "gcp"
            config.CLOUDQUEUE_MODE = "gcp"

    # -------------------------------------------------------------------------
    # 7. Publish Failure Error Handling
    # -------------------------------------------------------------------------
    def test_08_publish_failure_handling(self):
        """Verify that if Pub/Sub publishing fails, Flask returns HTTP 500 and updates status to Failed."""
        # Create a broken broker that raises RuntimeError on publish
        class BrokenPublisher:
            def publish(self, *args, **kwargs):
                raise RuntimeError("Pub/Sub connection timeout simulated")
            def topic_path(self, p, t):
                return f"projects/{p}/topics/{t}"

        pubsub_client.set_clients(publisher=BrokenPublisher(), subscriber=self.mock_broker)

        res = self.client.post("/submit", json={"job_name": "Failure Case", "workload_type": "CPU"})
        self.assertEqual(res.status_code, 500)
        data = res.get_json()
        self.assertIn("error", data)
        self.assertEqual(data["status"], "Failed")
        failed_id = data["job_id"]

        # Verify DB marks this job as Failed rather than Queued or Completed
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM jobs WHERE id = ?", (failed_id,))
        row = cursor.fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "Failed")

    # -------------------------------------------------------------------------
    # 8. Local Mode Experiment Execution & Comparison Regression
    # -------------------------------------------------------------------------
    def test_09_local_experiment_and_comparison(self):
        """Verify experiment running and comparison endpoint in local mode."""
        os.environ["CLOUDQUEUE_MODE"] = "local"
        config.CLOUDQUEUE_MODE = "local"

        try:
            # 1. Start experiment 1 (1 worker, 4 jobs)
            res1 = self.client.post("/experiments/run", json={"worker_count": 1, "job_count": 4, "workload_type": "CPU"})
            self.assertEqual(res1.status_code, 200)
            exp1_id = res1.get_json()["experiment_id"]

            # Wait for orchestrator thread to complete
            time.sleep(1.0)
            for _ in range(30):
                conn = get_db()
                cursor = conn.cursor()
                cursor.execute("SELECT status, throughput, completed_jobs FROM experiments WHERE experiment_id = ?", (exp1_id,))
                row = cursor.fetchone()
                conn.close()
                if row and row[0] == "Completed":
                    break
                time.sleep(0.2)

            self.assertEqual(row[0], "Completed")
            self.assertEqual(row[2], 4)

            # 2. Start experiment 2 (2 workers, 4 jobs)
            res2 = self.client.post("/experiments/run", json={"worker_count": 2, "job_count": 4, "workload_type": "CPU"})
            self.assertEqual(res2.status_code, 200)
            exp2_id = res2.get_json()["experiment_id"]

            time.sleep(1.0)
            for _ in range(30):
                conn = get_db()
                cursor = conn.cursor()
                cursor.execute("SELECT status, throughput, completed_jobs FROM experiments WHERE experiment_id = ?", (exp2_id,))
                row2 = cursor.fetchone()
                conn.close()
                if row2 and row2[0] == "Completed":
                    break
                time.sleep(0.2)

            self.assertEqual(row2[0], "Completed")
            self.assertEqual(row2[2], 4)

            # 3. Test Compare endpoint
            comp_res = self.client.post("/experiments/compare", json={"experiment_ids": [exp1_id, exp2_id]})
            self.assertEqual(comp_res.status_code, 200)
            comp_data = comp_res.get_json()
            self.assertTrue(comp_data.get("valid"))
            self.assertEqual(comp_data.get("job_count"), 4)
            self.assertEqual(comp_data.get("workload_type"), "CPU")
            self.assertEqual(len(comp_data.get("experiments")), 2)

        finally:
            os.environ["CLOUDQUEUE_MODE"] = "gcp"
            config.CLOUDQUEUE_MODE = "gcp"


if __name__ == "__main__":
    unittest.main()


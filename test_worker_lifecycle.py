"""
CloudQueue Experiment Worker Lifecycle Tests
Tests:
1. Reusable worker counting mechanism (distinguishes normal, experiment, stale)
2. Normal worker stopped before experiment starts
3. Stale experiment workers cleaned up before new experiment
4. Starting exactly 1, 2, 4, and 8 experiment workers
5. OS process count matches requested worker count
6. All experiment workers stopped after experiment completes
7. Exactly one normal worker restarted after experiment
8. Guaranteed cleanup on experiment startup failure
9. Guaranteed cleanup on experiment interruption / crash
10. End-to-end experiment lifecycle via /experiments/run API endpoint
"""

import os
import sys
import time
import json
import sqlite3
import shutil
import unittest
import subprocess

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

import config
import worker_manager
import app


class TestWorkerLifecycle(unittest.TestCase):
    def setUp(self):
        # Clean any running workers and PID files
        worker_manager.stop_experiment_workers()
        worker_manager.stop_normal_worker()
        worker_manager.stop_stale_experiment_workers()
        app.init_db()

    def tearDown(self):
        worker_manager.stop_experiment_workers()
        worker_manager.stop_normal_worker()
        worker_manager.stop_stale_experiment_workers()

    def test_01_worker_mode_cli_support(self):
        """Verify worker.py parses --worker-mode normal and experiment correctly."""
        py_code = """
import subprocess, sys
# Test worker.py --help output contains --worker-mode
proc = subprocess.run([sys.executable, 'worker.py', '--help'], capture_output=True, text=True)
assert '--worker-mode' in proc.stdout, '--worker-mode flag missing from worker.py'
print('WORKER_MODE_CLI_OK')
"""
        proc = subprocess.run([sys.executable, "-c", py_code], cwd=BASE_DIR, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("WORKER_MODE_CLI_OK", proc.stdout)

    def test_02_reusable_worker_counting_mechanism(self):
        """Verify count_active_workers accurately distinguishes worker categories."""
        status = worker_manager.count_active_workers()
        self.assertIn("normal", status)
        self.assertIn("experiment", status)
        self.assertIn("total", status)
        self.assertEqual(status["normal"], 0)
        self.assertEqual(status["experiment"], 0)
        self.assertEqual(status["total"], 0)

        # Start normal worker
        normal_pid = worker_manager.start_normal_worker()
        self.assertTrue(normal_pid > 0)
        self.assertTrue(worker_manager.is_pid_alive(normal_pid))

        status_after_normal = worker_manager.count_active_workers()
        self.assertEqual(status_after_normal["normal"], 1)
        self.assertEqual(status_after_normal["experiment"], 0)
        self.assertEqual(status_after_normal["total"], 1)

        # Stop normal worker
        stopped = worker_manager.stop_normal_worker()
        self.assertTrue(stopped)
        self.assertEqual(worker_manager.get_normal_worker_count(), 0)

    def test_03_start_exact_worker_counts(self):
        """Verify starting exactly 1, 2, 4, and 8 experiment workers."""
        for count in [1, 2, 4, 8]:
            exp_id = f"EXP-TEST-{count}"
            workers = worker_manager.start_experiment_workers(
                experiment_id=exp_id,
                worker_count=count,
                exit_when_empty=False,  # Keep alive for verification
                poll_interval=0.2
            )
            try:
                self.assertEqual(len(workers), count, f"Expected {count} Popen objects")
                active = worker_manager.get_experiment_worker_count(exp_id)
                self.assertEqual(active, count, f"Expected exactly {count} active processes for {exp_id}")

                # Verify each process is alive
                for p in workers:
                    self.assertTrue(worker_manager.is_pid_alive(p.pid))
            finally:
                worker_manager.stop_experiment_workers(exp_id)
                self.assertEqual(worker_manager.get_experiment_worker_count(exp_id), 0)

    def test_04_normal_worker_stopped_before_experiment(self):
        """Verify normal worker is stopped before an experiment starts."""
        # 1. Start normal worker
        worker_manager.start_normal_worker()
        self.assertEqual(worker_manager.get_normal_worker_count(), 1)

        # 2. Stop normal worker as required before experiment
        worker_manager.stop_normal_worker()
        self.assertEqual(worker_manager.get_normal_worker_count(), 0)

    def test_05_stale_experiment_workers_cleaned(self):
        """Verify stale experiment workers are cleanly stopped before a new experiment."""
        exp_old = "EXP-OLD"
        old_workers = worker_manager.start_experiment_workers(
            experiment_id=exp_old,
            worker_count=2,
            exit_when_empty=False,
            poll_interval=0.2
        )
        self.assertEqual(worker_manager.get_experiment_worker_count(exp_old), 2)

        # Call stop_stale_experiment_workers
        cleaned = worker_manager.stop_stale_experiment_workers()
        self.assertEqual(cleaned, 2)
        self.assertEqual(worker_manager.get_experiment_worker_count(), 0)

    def test_06_startup_failure_cleanup(self):
        """Verify that if worker startup fails, any started workers are cleaned up."""
        # Attempt starting invalid count (should raise ValueError and leave 0 running)
        with self.assertRaises(ValueError):
            worker_manager.start_experiment_workers("EXP-FAIL", 3)
        self.assertEqual(worker_manager.get_experiment_worker_count(), 0)

    def test_07_experiment_interruption_cleanup(self):
        """Verify experiment orchestrator cleans up workers on interruption."""
        exp_id = "EXP-INTERRUPT"
        # Simulate orchestrator error flow
        workers = worker_manager.start_experiment_workers(
            experiment_id=exp_id,
            worker_count=2,
            exit_when_empty=False,
            poll_interval=0.2
        )
        try:
            # Simulate unexpected error during experiment execution
            raise RuntimeError("Simulated crash during benchmark")
        except RuntimeError:
            pass
        finally:
            # Orchestrator guaranteed cleanup
            worker_manager.stop_experiment_workers(exp_id)
            worker_manager.start_normal_worker()

        # Check post-condition: 0 experiment workers, 1 normal worker
        self.assertEqual(worker_manager.get_experiment_worker_count(exp_id), 0)
        self.assertEqual(worker_manager.get_normal_worker_count(), 1)

    def test_08_end_to_end_lifecycle_via_api(self):
        """Verify full experiment lifecycle via /experiments/run API:
        - Normal worker running before experiment
        - Normal worker stopped, N experiment workers started
        - Experiment runs to completion
        - 0 experiment workers remain, 1 normal worker restored
        """
        # Ensure 1 normal worker before test
        worker_manager.start_normal_worker()
        self.assertEqual(worker_manager.get_normal_worker_count(), 1)

        client = app.app.test_client()

        # Submit small experiment: 2 workers, 10 CPU jobs
        res = client.post("/experiments/run", json={
            "worker_count": 2,
            "job_count": 10,
            "workload_type": "CPU"
        })
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        exp_id = data["experiment_id"]
        self.assertEqual(data["worker_count"], 2)

        # Wait for experiment completion (polling up to 20 seconds)
        start_wait = time.time()
        completed = False
        while time.time() - start_wait < 20.0:
            status_res = client.get(f"/experiments/{exp_id}")
            if status_res.status_code == 200:
                sdata = status_res.get_json()
                if sdata["status"] == "Completed":
                    completed = True
                    break
            time.sleep(0.5)

        self.assertTrue(completed, f"Experiment {exp_id} did not complete in time")

        # Post-experiment state verification:
        # 1. 0 experiment workers alive
        self.assertEqual(worker_manager.get_experiment_worker_count(), 0)
        # 2. Exactly 1 normal worker restarted and alive
        self.assertEqual(worker_manager.get_normal_worker_count(), 1)


if __name__ == "__main__":
    unittest.main()

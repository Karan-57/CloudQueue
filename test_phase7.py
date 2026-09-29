"""
Phase 7 End-to-End Validation & Benchmark Tests
Tests:
1. gcp/run_phase7.sh exists, executable bit, LF endings, and bash -n syntax validation.
2. gcp/run_phase7.py exists, valid Python syntax, correct argument parsing.
3. Report generator produces all Phase 7I required sections.
4. Experiment comparison logic and schema compatibility.
5. Dry-run / mock end-to-end execution of Phase 7 workflow.
"""

import os
import platform
import subprocess
import sys
import unittest

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GIT_BASH = r"C:\Program Files\Git\bin\bash.exe"


class TestPhase7Validation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script_sh = os.path.join(BASE_DIR, "gcp", "run_phase7.sh")
        cls.script_py = os.path.join(BASE_DIR, "gcp", "run_phase7.py")

    def test_01_script_sh_exists_and_syntax(self):
        """Verify gcp/run_phase7.sh exists, has LF endings, and passes bash syntax validation."""
        self.assertTrue(os.path.isfile(self.script_sh), "gcp/run_phase7.sh must exist")
        with open(self.script_sh, "rb") as f:
            content = f.read()
        self.assertNotIn(b"\r\n", content, "Script must use pure LF line endings")
        self.assertTrue(content.startswith(b"#!/usr/bin/env bash"), "Script must start with bash shebang")

        bash_cmd = "bash"
        if platform.system() == "Windows" and os.path.exists(GIT_BASH):
            bash_cmd = GIT_BASH

        res = subprocess.run([bash_cmd, "-n", self.script_sh], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0, f"Syntax error in gcp/run_phase7.sh: {res.stderr}")

    def test_02_script_py_exists_and_help(self):
        """Verify gcp/run_phase7.py exists and responds to --help."""
        self.assertTrue(os.path.isfile(self.script_py), "gcp/run_phase7.py must exist")
        res = subprocess.run([sys.executable, self.script_py, "--help"], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0)
        self.assertIn("CloudQueue Phase 7 Real-GCP End-to-End Benchmark Runner", res.stdout)
        self.assertIn("--job-count", res.stdout)
        self.assertIn("--workload", res.stdout)
        self.assertIn("--workers", res.stdout)

    def test_03_report_generator_sections(self):
        """Verify generate_report produces all required Phase 7I sections."""
        from gcp.run_phase7 import generate_report

        env_meta = {
            "project_id": "test-project-123",
            "machine_type": "e2-standard-4",
            "python_version": "3.11.2",
            "mode": "gcp",
            "topic": "cloudqueue-jobs",
            "subscription": "cloudqueue-worker-sub",
            "db_path": "database/cloudqueue.db"
        }
        smoke_meta = {
            "job_id": 1,
            "worker_id": "smoke-worker-1",
            "status": "Completed",
            "wait_time": 0.05,
            "proc_time": 0.1,
            "total_latency": 0.15
        }
        perf_results = [
            {
                "experiment_id": "EXP-001",
                "workers": 1,
                "jobs": 100,
                "workload": "CPU",
                "total_time": 10.5,
                "throughput": 9.52,
                "avg_wait": 4.5,
                "avg_proc": 0.1,
                "avg_latency": 4.6,
                "completed": 100,
                "failed": 0,
                "distribution": {"worker-1": 100}
            },
            {
                "experiment_id": "EXP-002",
                "workers": 2,
                "jobs": 100,
                "workload": "CPU",
                "total_time": 5.4,
                "throughput": 18.52,
                "avg_wait": 2.2,
                "avg_proc": 0.1,
                "avg_latency": 2.3,
                "completed": 100,
                "failed": 0,
                "distribution": {"worker-1": 52, "worker-2": 48}
            }
        ]
        multi_dist = {"SharedSub-Worker-1": 10, "SharedSub-Worker-2": 10}
        backup_meta = {"status": "Success", "backup_file": "backups/cloudqueue.db"}
        two_vm_note = "Two-VM demonstration skipped: Google Cloud Skills Boost lab session provides a single VM quota."

        report = generate_report(env_meta, smoke_meta, perf_results, multi_dist, backup_meta, two_vm_note)

        # Check required sections per Phase 7I
        self.assertIn("## 1. Environment", report)
        self.assertIn("## 2. Real Pub/Sub Verification", report)
        self.assertIn("## 3. Controlled Performance Experiment Table", report)
        self.assertIn("## 4. Worker Distribution for Each Experiment", report)
        self.assertIn("## 5. Duplicate-Processing Observation", report)
        self.assertIn("## 6. Experiment Comparison Verification", report)
        self.assertIn("## 7. Two-VM Architecture Demonstration", report)
        self.assertIn("## 8. GitHub Persistence Result", report)
        self.assertIn("## 9. Local Restoration Result", report)
        self.assertIn("## 10. Test Count & Regression Status", report)
        self.assertIn("## 11. Environment Limitations & Observations", report)

        # Check key phrasing
        self.assertIn("Zero duplicate processing was observed in the tested runs.", report)
        self.assertIn("cloudqueue-worker-sub", report)
        self.assertIn("cloudqueue-jobs", report)

    def test_04_end_to_end_mock_validation(self):
        """Verify full automated execution of run_phase7.py with --mock --skip-backup."""
        cmd = [
            sys.executable,
            self.script_py,
            "--mock",
            "--job-count", "2",
            "--workers", "1,2",
            "--skip-backup"
        ]
        res = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, timeout=60.0)
        self.assertEqual(res.returncode, 0, f"run_phase7.py failed: {res.stderr}\nOutput: {res.stdout}")
        self.assertIn("PHASE 7 VALIDATION COMPLETE", res.stdout)
        self.assertIn("Zero duplicate processing was observed in the tested runs.", res.stdout)
        self.assertIn("SharedSub-Worker", res.stdout)


if __name__ == "__main__":
    unittest.main()

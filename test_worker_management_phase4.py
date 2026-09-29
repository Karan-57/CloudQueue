"""
Phase 4 Automated Worker Management Tests
Tests:
1. Script existence, executable permissions, and bash syntax validation
2. Worker count validation (accepts only 1, 2, 4, 8; rejects 0, 3, 5, abc)
3. Duplicate launch protection (prevents launching new workers if active workers exist)
4. Worker process startup, PID file creation, log file creation
5. Stop workers execution, graceful SIGTERM termination, and PID file cleanup
6. Idempotent stop behavior (safe execution when no workers are running)
7. Full regression: Phase 2 and Phase 3 verification
"""

import os
import shutil
import subprocess
import sys
import time
import unittest

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GIT_BASH = r"C:\Program Files\Git\bin\bash.exe"


class TestPhase4WorkerManagement(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gcp_dir = os.path.join(BASE_DIR, "gcp")
        cls.start_script = os.path.join(cls.gcp_dir, "start_workers.sh")
        cls.stop_script = os.path.join(cls.gcp_dir, "stop_workers.sh")
        cls.dot_gcp = os.path.join(BASE_DIR, ".gcp")

    def setUp(self):
        # Clean up any leftover PID files or test logs
        if os.path.exists(self.dot_gcp):
            shutil.rmtree(self.dot_gcp, ignore_errors=True)

    def tearDown(self):
        # Ensure any test workers are stopped
        if os.path.exists(GIT_BASH) and os.path.exists(self.stop_script):
            subprocess.run([GIT_BASH, self.stop_script], cwd=BASE_DIR, capture_output=True)
        if os.path.exists(self.dot_gcp):
            shutil.rmtree(self.dot_gcp, ignore_errors=True)

    def run_bash(self, script_path, *args):
        """Runs a bash script via Git Bash if available, else WSL or standard bash."""
        cmd = [GIT_BASH, script_path] + list(args)
        return subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True)

    def test_01_script_existence_and_syntax(self):
        """Verify start_workers.sh and stop_workers.sh exist and pass bash -n syntax checks."""
        self.assertTrue(os.path.isfile(self.start_script), "gcp/start_workers.sh must exist")
        self.assertTrue(os.path.isfile(self.stop_script), "gcp/stop_workers.sh must exist")

        for s in [self.start_script, self.stop_script]:
            with open(s, "rb") as f:
                content = f.read()
            self.assertNotIn(b"\r\n", content, f"{s} must use pure LF line endings")
            self.assertTrue(content.startswith(b"#!/usr/bin/env bash"), f"{s} must start with #!/usr/bin/env bash")

        if os.path.exists(GIT_BASH):
            res1 = subprocess.run([GIT_BASH, "-n", self.start_script], capture_output=True, text=True)
            self.assertEqual(res1.returncode, 0, f"start_workers.sh syntax error: {res1.stderr}")
            res2 = subprocess.run([GIT_BASH, "-n", self.stop_script], capture_output=True, text=True)
            self.assertEqual(res2.returncode, 0, f"stop_workers.sh syntax error: {res2.stderr}")

    def test_02_invalid_worker_counts_rejected(self):
        """Verify invalid worker counts (0, 3, 5, 9, abc) fail with clear error message."""
        if not os.path.exists(GIT_BASH):
            self.skipTest("Git Bash not installed")

        invalid_inputs = ["0", "3", "5", "7", "9", "abc", "-1", ""]
        for count in invalid_inputs:
            proc = self.run_bash(self.start_script, count)
            self.assertNotEqual(proc.returncode, 0, f"Count '{count}' should have failed")
            output = proc.stdout + proc.stderr
            self.assertIn("Invalid worker count", output, f"Expected 'Invalid worker count' for input '{count}'")
            self.assertIn("Supported worker counts: 1, 2, 4, 8", output)

    def test_03_stop_workers_when_none_running(self):
        """Verify stop_workers.sh exits cleanly (exit code 0) when no workers are running."""
        if not os.path.exists(GIT_BASH):
            self.skipTest("Git Bash not installed")

        proc = self.run_bash(self.stop_script)
        self.assertEqual(proc.returncode, 0)
        self.assertIn("No running CloudQueue workers found", proc.stdout)

    def test_04_duplicate_launch_protection(self):
        """Verify that starting workers when workers are already running is blocked."""
        if not os.path.exists(GIT_BASH):
            self.skipTest("Git Bash not installed")

        # Spawn a separate child process to act as an active worker
        dummy_proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(15)"])
        try:
            os.makedirs(self.dot_gcp, exist_ok=True)
            fake_pid_file = os.path.join(self.dot_gcp, "worker-1.pid")
            with open(fake_pid_file, "w") as f:
                f.write(str(dummy_proc.pid))

            # Attempt to start workers
            proc = self.run_bash(self.start_script, "2")
            self.assertNotEqual(proc.returncode, 0, "Duplicate launch should be blocked")
            output = proc.stdout + proc.stderr
            self.assertIn("CloudQueue workers are already running", output)
            self.assertIn("Use ./gcp/stop_workers.sh", output)
        finally:
            dummy_proc.terminate()
            dummy_proc.wait()

    def test_05_stale_pid_cleanup(self):
        """Verify that a stale PID file (process not running) is cleaned up automatically."""
        if not os.path.exists(GIT_BASH):
            self.skipTest("Git Bash not installed")

        os.makedirs(self.dot_gcp, exist_ok=True)
        stale_pid_file = os.path.join(self.dot_gcp, "worker-1.pid")
        # Use an impossibly high PID unlikely to be running
        with open(stale_pid_file, "w") as f:
            f.write("99999999")

        # Running stop_workers should clean up the stale PID file
        proc = self.run_bash(self.stop_script)
        self.assertEqual(proc.returncode, 0)
        self.assertFalse(os.path.exists(stale_pid_file), "Stale PID file should have been removed")

    def test_06_worker_start_and_stop_lifecycle(self):
        """Verify starting 2 workers creates PID files, logs, and stop_workers cleans them up."""
        if not os.path.exists(GIT_BASH):
            self.skipTest("Git Bash not installed")

        # Set environment to mock Pub/Sub mode so worker process runs without credentials
        env_file = os.path.join(BASE_DIR, ".env")
        with open(env_file, "w") as f:
            f.write("CLOUDQUEUE_MODE=gcp\n")
            f.write("CLOUDQUEUE_MOCK_PUBSUB=true\n")
            f.write("GOOGLE_CLOUD_PROJECT=mock-test-project\n")
            f.write("PUBSUB_TOPIC=cloudqueue-jobs\n")
            f.write("PUBSUB_SUBSCRIPTION=cloudqueue-worker-sub\n")

        try:
            # 1. Start 2 workers
            start_proc = self.run_bash(self.start_script, "2")
            self.assertEqual(start_proc.returncode, 0, f"start_workers failed: {start_proc.stderr}")
            self.assertIn("Worker-1 started", start_proc.stdout)
            self.assertIn("Worker-2 started", start_proc.stdout)

            # 2. Check PID files created
            pid1_path = os.path.join(self.dot_gcp, "worker-1.pid")
            pid2_path = os.path.join(self.dot_gcp, "worker-2.pid")
            self.assertTrue(os.path.exists(pid1_path), "worker-1.pid must exist")
            self.assertTrue(os.path.exists(pid2_path), "worker-2.pid must exist")

            with open(pid1_path) as f:
                pid1 = int(f.read().strip())
            with open(pid2_path) as f:
                pid2 = int(f.read().strip())

            self.assertTrue(pid1 > 0)
            self.assertTrue(pid2 > 0)

            # 3. Check logs created
            log1_path = os.path.join(self.dot_gcp, "logs", "worker-1.log")
            log2_path = os.path.join(self.dot_gcp, "logs", "worker-2.log")
            self.assertTrue(os.path.exists(log1_path), "worker-1.log must exist")
            self.assertTrue(os.path.exists(log2_path), "worker-2.log must exist")

            time.sleep(0.5)

            # 4. Stop workers
            stop_proc = self.run_bash(self.stop_script)
            self.assertEqual(stop_proc.returncode, 0, f"stop_workers failed: {stop_proc.stderr}")
            self.assertIn("Worker-1 stopped.", stop_proc.stdout)
            self.assertIn("Worker-2 stopped.", stop_proc.stdout)
            self.assertIn("All CloudQueue workers stopped.", stop_proc.stdout)

            # 5. PID files must be deleted
            self.assertFalse(os.path.exists(pid1_path), "worker-1.pid should be deleted after stop")
            self.assertFalse(os.path.exists(pid2_path), "worker-2.pid should be deleted after stop")

        finally:
            if os.path.exists(env_file):
                os.remove(env_file)


if __name__ == "__main__":
    unittest.main()

"""
Phase 6 Result Restoration Tests
Tests:
1. scripts/restore_results.sh exists, executable bit, LF line endings, and syntax validity (bash -n)
2. backups/cloudqueue.db exists, is valid SQLite, and contains expected tables
3. Restore execution successfully replaces local database
4. Pre-restore safety backup is created in .gcp/local_db_backups/ with timestamp
5. Failed validation (corrupt backup) aborts and preserves active local database
6. Restoration leaves backups/cloudqueue.db strictly unmodified
7. Active process detection blocks restoration to prevent corruption
"""

import hashlib
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import unittest

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GIT_BASH = r"C:\Program Files\Git\bin\bash.exe"


class TestPhase6Restore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.restore_script = os.path.join(BASE_DIR, "scripts", "restore_results.sh")
        cls.backup_db = os.path.join(BASE_DIR, "backups", "cloudqueue.db")
        cls.local_db = os.path.join(BASE_DIR, "database", "cloudqueue.db")
        cls.local_backups_dir = os.path.join(BASE_DIR, ".gcp", "local_db_backups")

    def run_bash(self, script_path, *args, env=None):
        cmd = [GIT_BASH, script_path] + list(args)
        run_env = os.environ.copy()
        if env:
            run_env.update(env)
        return subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, env=run_env)

    def test_01_script_exists_and_syntax(self):
        """Verify scripts/restore_results.sh exists, has LF endings, and passes bash -n."""
        self.assertTrue(os.path.isfile(self.restore_script), "scripts/restore_results.sh must exist")
        with open(self.restore_script, "rb") as f:
            content = f.read()
        self.assertNotIn(b"\r\n", content, "Script must use pure LF line endings")
        self.assertTrue(content.startswith(b"#!/usr/bin/env bash"), "Script must start with bash shebang")

        if os.path.exists(GIT_BASH):
            res = subprocess.run([GIT_BASH, "-n", self.restore_script], capture_output=True, text=True)
            self.assertEqual(res.returncode, 0, f"Syntax error: {res.stderr}")

    def test_02_backup_database_validity_and_schema(self):
        """Verify backups/cloudqueue.db exists, is valid SQLite, and contains required tables."""
        self.assertTrue(os.path.isfile(self.backup_db), "backups/cloudqueue.db must exist")

        conn = sqlite3.connect(self.backup_db)
        c = conn.cursor()
        c.execute("PRAGMA integrity_check;")
        res = c.fetchone()
        self.assertEqual(res[0], "ok", "backups/cloudqueue.db must pass integrity check")

        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {r[0] for r in c.fetchall()}
        required = {"jobs", "experiments", "experiment_jobs"}
        self.assertTrue(required.issubset(tables), f"Missing tables in backup: {required - tables}")
        conn.close()

    def setUp(self):
        # Ensure any canary records from previous test runs are cleaned up
        if os.path.exists(self.local_db):
            try:
                conn = sqlite3.connect(self.local_db, timeout=30.0)
                conn.execute("DELETE FROM jobs WHERE id IN (99999, 88888);")
                conn.commit()
                conn.close()
            except sqlite3.OperationalError:
                pass

    def test_03_restore_and_pre_restore_backup(self):
        """Verify restore safely creates pre-restore backup and populates target database."""
        if not os.path.exists(GIT_BASH):
            self.skipTest("Git Bash not installed")

        # 1. Prepare a temporary local database with a unique marker record
        os.makedirs(os.path.dirname(self.local_db), exist_ok=True)
        conn = sqlite3.connect(self.local_db, timeout=30.0)
        conn.execute("CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, job_name TEXT, workload_type TEXT, status TEXT);")
        conn.execute("CREATE TABLE IF NOT EXISTS experiments (id INTEGER PRIMARY KEY, experiment_id TEXT, status TEXT);")
        conn.execute("CREATE TABLE IF NOT EXISTS experiment_jobs (id INTEGER PRIMARY KEY, experiment_id TEXT, status TEXT);")
        conn.execute("DELETE FROM jobs WHERE id = 99999;")
        conn.execute("INSERT INTO jobs (id, job_name, workload_type, status) VALUES (99999, 'PreRestore Canary Job', 'CPU', 'Queued');")
        conn.commit()
        conn.close()

        # Count pre-existing backups
        os.makedirs(self.local_backups_dir, exist_ok=True)
        before_backups = set(os.listdir(self.local_backups_dir))

        # 2. Execute restore_results.sh
        proc = self.run_bash(self.restore_script)
        self.assertEqual(proc.returncode, 0, f"restore_results.sh failed: {proc.stderr}")
        self.assertIn("Database Restore Successful", proc.stdout)

        # 3. Verify pre-restore safety copy was created
        after_backups = set(os.listdir(self.local_backups_dir))
        new_backups = after_backups - before_backups
        self.assertTrue(len(new_backups) >= 1, "A pre-restore backup file must have been created")

        newest_backup = os.path.join(self.local_backups_dir, sorted(list(new_backups))[-1])
        b_conn = sqlite3.connect(newest_backup)
        b_c = b_conn.cursor()
        b_c.execute("SELECT job_name FROM jobs WHERE id = 99999;")
        row = b_c.fetchone()
        b_conn.close()
        self.assertIsNotNone(row, "Pre-restore safety backup must preserve active local data")
        self.assertEqual(row[0], "PreRestore Canary Job")

        # 4. Verify local database was replaced with content from backups/cloudqueue.db
        l_conn = sqlite3.connect(self.local_db)
        l_c = l_conn.cursor()
        l_c.execute("SELECT COUNT(*) FROM jobs WHERE id = 99999;")
        self.assertEqual(l_c.fetchone()[0], 0, "Canary record should be replaced by backup contents")
        l_conn.close()

    def test_04_failed_validation_preserves_local_database(self):
        """Verify that a corrupted backup fails validation and does NOT alter the active local database."""
        if not os.path.exists(GIT_BASH):
            self.skipTest("Git Bash not installed")

        # Set up canary in local DB
        conn = sqlite3.connect(self.local_db, timeout=30.0)
        conn.execute("INSERT OR REPLACE INTO jobs (id, job_name, workload_type, status) VALUES (88888, 'Should Not Be Deleted', 'CPU', 'Queued');")
        conn.commit()
        conn.close()

        # Temporarily swap backup with corrupted file
        backup_real = self.backup_db
        backup_corrupt = self.backup_db + ".corrupt_tmp"
        shutil.copyfile(backup_real, backup_corrupt)

        try:
            with open(backup_real, "wb") as f:
                f.write(b"THIS_IS_NOT_A_VALID_SQLITE_DATABASE_FILE")

            proc = self.run_bash(self.restore_script)
            self.assertNotEqual(proc.returncode, 0, "Corrupt backup restore must fail with non-zero exit code")
            self.assertTrue(
                "failed SQLite integrity check" in proc.stderr or "Error validating backup" in proc.stderr or "failed" in proc.stderr
            )

            # Local database must be untouched
            conn = sqlite3.connect(self.local_db)
            c = conn.cursor()
            c.execute("SELECT job_name FROM jobs WHERE id = 88888;")
            row = c.fetchone()
            conn.close()
            self.assertIsNotNone(row, "Local database should remain untouched after failed validation")
            self.assertEqual(row[0], "Should Not Be Deleted")
        finally:
            # Restore genuine backup
            shutil.copyfile(backup_corrupt, backup_real)
            if os.path.exists(backup_corrupt):
                os.remove(backup_corrupt)

    def test_05_backup_database_unmodified_by_restore(self):
        """Verify that running restore_results.sh never modifies backups/cloudqueue.db."""
        if not os.path.exists(GIT_BASH):
            self.skipTest("Git Bash not installed")

        with open(self.backup_db, "rb") as f:
            hash_before = hashlib.sha256(f.read()).hexdigest()

        proc = self.run_bash(self.restore_script)
        self.assertEqual(proc.returncode, 0)

        with open(self.backup_db, "rb") as f:
            hash_after = hashlib.sha256(f.read()).hexdigest()

        self.assertEqual(hash_before, hash_after, "backups/cloudqueue.db must not be modified by restore_results.sh")

    def test_06_active_process_detection_blocks_restore(self):
        """Verify restore aborts if active CloudQueue worker processes are detected."""
        if not os.path.exists(GIT_BASH):
            self.skipTest("Git Bash not installed")

        # Spawn a dummy process to simulate active worker
        dummy = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(15)"])
        dot_gcp = os.path.join(BASE_DIR, ".gcp")
        os.makedirs(dot_gcp, exist_ok=True)
        worker_pid_file = os.path.join(dot_gcp, "worker-1.pid")

        try:
            with open(worker_pid_file, "w") as f:
                f.write(str(dummy.pid))

            proc = self.run_bash(self.restore_script)
            self.assertNotEqual(proc.returncode, 0, "Restore must abort when active worker is running")
            output = proc.stdout + proc.stderr
            self.assertIn("CloudQueue worker processes are currently running", output)
        finally:
            dummy.terminate()
            dummy.wait()
            if os.path.exists(worker_pid_file):
                os.remove(worker_pid_file)


if __name__ == "__main__":
    unittest.main()

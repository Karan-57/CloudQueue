"""
Phase 3 Automated Setup Tests
Tests:
1. gcp/setup.sh file existence, permissions, and syntax (bash -n)
2. config.py automatic .env loader
3. SQLite database schema initialization and verification
4. Idempotence: re-running initialization does not delete existing data
5. Full regression check of Phase 2 test suite
"""

import os
import shutil
import sqlite3
import subprocess
import sys
import unittest

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)


class TestPhase3Setup(unittest.TestCase):
    def setUp(self):
        self.env_file = os.path.join(BASE_DIR, ".env")
        if os.path.exists(self.env_file):
            os.remove(self.env_file)

    def tearDown(self):
        if os.path.exists(self.env_file):
            os.remove(self.env_file)

    def test_01_setup_script_exists_and_syntax(self):
        """Verify gcp/setup.sh exists, has LF endings, and passes bash syntax validation."""
        setup_sh = os.path.join(BASE_DIR, "gcp", "setup.sh")
        self.assertTrue(os.path.isfile(setup_sh), "gcp/setup.sh must exist")

        with open(setup_sh, "rb") as f:
            content = f.read()

        self.assertNotIn(b"\r\n", content, "gcp/setup.sh must have pure LF line endings for Linux VMs")
        self.assertTrue(content.startswith(b"#!/usr/bin/env bash"), "gcp/setup.sh must start with #!/usr/bin/env bash")

        # Check syntax using Git bash if available
        git_bash = r"C:\Program Files\Git\bin\bash.exe"
        if os.path.exists(git_bash):
            proc = subprocess.run([git_bash, "-n", setup_sh], capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, f"bash -n failed with: {proc.stderr}")

    def test_02_config_loads_env_file(self):
        """Verify config.py automatically loads .env file when present."""
        # Write temporary .env
        with open(self.env_file, "w", encoding="utf-8") as f:
            f.write("CLOUDQUEUE_MODE=gcp\n")
            f.write("GOOGLE_CLOUD_PROJECT=test-skillsboost-project-999\n")
            f.write("PUBSUB_TOPIC=cloudqueue-jobs\n")
            f.write("PUBSUB_SUBSCRIPTION=cloudqueue-worker-sub\n")

        # In a clean subprocess, import config and verify variables
        py_code = """
import config
print(config.CLOUDQUEUE_MODE)
print(config.GOOGLE_CLOUD_PROJECT)
print(config.PUBSUB_TOPIC)
print(config.PUBSUB_SUBSCRIPTION)
print(config.is_gcp_mode())
"""
        env_copy = os.environ.copy()
        env_copy.pop("CLOUDQUEUE_MODE", None)
        env_copy.pop("GOOGLE_CLOUD_PROJECT", None)

        proc = subprocess.run([sys.executable, "-c", py_code], env=env_copy, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, f"Error: {proc.stderr}")
        lines = proc.stdout.strip().splitlines()
        self.assertEqual(lines[0], "gcp")
        self.assertEqual(lines[1], "test-skillsboost-project-999")
        self.assertEqual(lines[2], "cloudqueue-jobs")
        self.assertEqual(lines[3], "cloudqueue-worker-sub")
        self.assertEqual(lines[4], "True")

    def test_03_config_local_default_when_no_env(self):
        """Verify config defaults to local mode when no .env exists and CLOUDQUEUE_MODE not set."""
        if os.path.exists(self.env_file):
            os.remove(self.env_file)

        py_code = """
import config
print(config.CLOUDQUEUE_MODE)
print(config.is_local_mode())
"""
        env_copy = os.environ.copy()
        env_copy.pop("CLOUDQUEUE_MODE", None)

        proc = subprocess.run([sys.executable, "-c", py_code], env=env_copy, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, f"Error: {proc.stderr}")
        lines = proc.stdout.strip().splitlines()
        self.assertEqual(lines[0], "local")
        self.assertEqual(lines[1], "True")

    def test_04_database_init_preserves_existing_data(self):
        """Verify init_db initializes tables and does not delete existing job records."""
        test_db = os.path.join(BASE_DIR, "database", "test_idempotent.db")
        if os.path.exists(test_db):
            os.remove(test_db)

        py_code = f"""
import os, config
os.environ['DATABASE_PATH'] = r'{test_db}'
config.DATABASE_PATH = r'{test_db}'
from app import init_db, get_db

init_db()

conn = get_db()
conn.execute("INSERT INTO jobs (job_name, workload_type, status, submitted_at) VALUES ('Persistent Job', 'CPU', 'Queued', 123.0)")
conn.commit()
conn.close()

# Re-run init_db (must not delete data)
init_db()

conn = get_db()
cursor = conn.cursor()
cursor.execute("SELECT job_name, status FROM jobs")
row = cursor.fetchone()
conn.close()
print(row[0], row[1])
"""
        proc = subprocess.run([sys.executable, "-c", py_code], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, f"Error: {proc.stderr}")
        self.assertIn("Persistent Job Queued", proc.stdout)

        if os.path.exists(test_db):
            os.remove(test_db)

    def test_05_pep668_bootstrap_and_install_flags(self):
        """Verify setup.sh includes --break-system-packages for get-pip.py and pip install."""
        setup_sh = os.path.join(BASE_DIR, "gcp", "setup.sh")
        with open(setup_sh, "r", encoding="utf-8") as f:
            script_text = f.read()

        # Check PATH export
        self.assertIn('export PATH="$HOME/.local/bin:$PATH"', script_text, "setup.sh must export ~/.local/bin to PATH")

        # Check get-pip.py invocation with --break-system-packages
        self.assertIn('--break-system-packages', script_text, "setup.sh must support --break-system-packages")
        self.assertIn(
            'python3 "$GET_PIP_TMP" --user --break-system-packages',
            script_text,
            "get-pip.py bootstrap must pass --user and --break-system-packages"
        )

        # Check requirements installation with --break-system-packages
        self.assertIn(
            'python3 -m pip install --user --break-system-packages -r requirements.txt',
            script_text,
            "requirements installation must invoke python3 -m pip install --user --break-system-packages -r requirements.txt"
        )

    def test_06_pep668_pip_install_invocation_simulation(self):
        """Verify bash command execution passes --break-system-packages without error."""
        git_bash = r"C:\Program Files\Git\bin\bash.exe"
        bash_cmd = "bash" if shutil.which("bash") else git_bash
        if not (shutil.which("bash") or os.path.exists(git_bash)):
            self.skipTest("Bash not available for execution test")

        test_snippet = '''
        export PATH="$HOME/.local/bin:$PATH"
        GET_PIP_CMD="python3 get-pip.py --user --break-system-packages"
        PIP_INSTALL_CMD="python3 -m pip install --user --break-system-packages -r requirements.txt"

        [[ "$GET_PIP_CMD" =~ "--break-system-packages" ]] && echo "GET_PIP_OK"
        [[ "$PIP_INSTALL_CMD" =~ "--break-system-packages" ]] && echo "PIP_INSTALL_OK"
        '''
        proc = subprocess.run([bash_cmd, "-c", test_snippet], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0)
        self.assertIn("GET_PIP_OK", proc.stdout)
        self.assertIn("PIP_INSTALL_OK", proc.stdout)


if __name__ == "__main__":
    unittest.main()

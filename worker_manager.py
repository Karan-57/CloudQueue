"""
CloudQueue Worker Process Manager
Handles lifecycle, verification, and cleanup for normal and experiment worker processes.
"""

import os
import sys
import time
import json
import signal
import subprocess
import glob
from typing import Dict, List, Optional, Any, Tuple
import config

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PID_DIR = os.path.join(BASE_DIR, ".gcp")
LOG_DIR = os.path.join(BASE_DIR, ".gcp", "logs")


def _ensure_dirs():
    os.makedirs(PID_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)


def is_pid_alive(pid: int) -> bool:
    """
    Safely checks if a process is alive across Windows and Unix.
    On Windows, uses kernel32 OpenProcess with query rights to avoid terminating the process.
    """
    if not pid or pid <= 0:
        return False

    if sys.platform == "win32":
        try:
            import ctypes
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid)
            )
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong()
                if ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    # STILL_ACTIVE = 259
                    return exit_code.value == 259
                return False
            finally:
                ctypes.windll.kernel32.CloseHandle(handle)
        except Exception:
            return False
    else:
        try:
            os.kill(int(pid), 0)
            return True
        except (OSError, ProcessLookupError):
            return False


def stop_pid(pid: int, timeout: float = 3.0) -> bool:
    """Gracefully stops a process by PID, escalating to force kill if needed."""
    if not is_pid_alive(pid):
        return True

    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                timeout=timeout + 2.0
            )
        except Exception:
            pass
        return not is_pid_alive(pid)
    else:
        try:
            os.kill(int(pid), signal.SIGTERM)
        except OSError:
            return True

        start = time.time()
        while time.time() - start < timeout:
            if not is_pid_alive(pid):
                return True
            time.sleep(0.1)

        try:
            os.kill(int(pid), signal.SIGKILL)
        except OSError:
            pass

        return not is_pid_alive(pid)


def _write_pid_file(filepath: str, pid: int, metadata: Dict[str, Any]):
    """Writes PID on the first line for shell script compatibility, and JSON metadata on the second line."""
    _ensure_dirs()
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"{pid}\n")
        f.write(json.dumps(metadata) + "\n")


def _read_pid_file(filepath: str) -> Tuple[Optional[int], Dict[str, Any]]:
    """Reads PID and metadata from a PID file. Returns (pid, metadata)."""
    if not os.path.isfile(filepath):
        return None, {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if not lines:
            return None, {}
        pid = int(lines[0].strip())
        meta = {}
        if len(lines) > 1 and lines[1].strip():
            try:
                meta = json.loads(lines[1].strip())
            except Exception:
                pass
        return pid, meta
    except Exception:
        return None, {}


def _spawn_worker(cmd: List[str], log_file: str) -> subprocess.Popen:
    """Spawns a worker subprocess redirecting output to the specified log file."""
    with open(log_file, "a", encoding="utf-8") as log_fp:
        kwargs: Dict[str, Any] = {
            "stdout": log_fp,
            "stderr": log_fp,
            "cwd": BASE_DIR,
            "env": os.environ.copy()
        }
        if sys.platform != "win32":
            kwargs["start_new_session"] = True
        return subprocess.Popen(cmd, **kwargs)


# ------------------------------------------------------------------------------
# Normal Worker Lifecycle
# ------------------------------------------------------------------------------

NORMAL_PID_FILE = os.path.join(PID_DIR, "worker-normal.pid")


def get_normal_worker_count() -> int:
    """Returns 1 if the normal worker is alive, else 0 (cleaning stale PID files)."""
    pid, meta = _read_pid_file(NORMAL_PID_FILE)
    if pid is not None:
        if is_pid_alive(pid):
            return 1
        try:
            os.remove(NORMAL_PID_FILE)
        except Exception:
            pass
    return 0


def get_normal_worker_pid() -> Optional[int]:
    pid, _ = _read_pid_file(NORMAL_PID_FILE)
    if pid is not None and is_pid_alive(pid):
        return pid
    return None


def start_normal_worker() -> Optional[int]:
    """Starts exactly ONE normal worker if not already running."""
    _ensure_dirs()
    existing_pid = get_normal_worker_pid()
    if existing_pid:
        return existing_pid

    if os.path.exists(NORMAL_PID_FILE):
        try:
            os.remove(NORMAL_PID_FILE)
        except Exception:
            pass

    log_file = os.path.join(LOG_DIR, "worker-normal.log")
    cmd = [
        sys.executable,
        os.path.join(BASE_DIR, "worker.py"),
        "--worker-id", "normal-worker-1",
        "--worker-mode", "normal",
        "--mode", config.CLOUDQUEUE_MODE,
        "--poll-interval", "0.5"
    ]

    proc = _spawn_worker(cmd, log_file)
    pid = proc.pid

    metadata = {
        "worker_mode": "normal",
        "worker_id": "normal-worker-1",
        "started_at": time.time(),
        "cmd": cmd
    }
    _write_pid_file(NORMAL_PID_FILE, pid, metadata)

    time.sleep(0.2)
    if not is_pid_alive(pid):
        try:
            os.remove(NORMAL_PID_FILE)
        except Exception:
            pass
        raise RuntimeError("Normal worker process failed to start or exited immediately.")

    return pid


def stop_normal_worker(timeout: float = 5.0) -> bool:
    """Stops the active normal worker and verifies it has stopped."""
    pid, _ = _read_pid_file(NORMAL_PID_FILE)
    if pid is not None:
        stop_pid(pid, timeout=timeout)
        try:
            os.remove(NORMAL_PID_FILE)
        except Exception:
            pass
    return get_normal_worker_count() == 0


def ensure_normal_worker_running() -> Optional[int]:
    """Ensures normal worker is running if no experiment is currently active."""
    if get_experiment_worker_count() > 0:
        return None
    if get_normal_worker_count() == 0:
        return start_normal_worker()
    return get_normal_worker_pid()


# ------------------------------------------------------------------------------
# Experiment Worker Lifecycle
# ------------------------------------------------------------------------------

def stop_stale_experiment_workers(timeout: float = 5.0) -> int:
    """Stops leftover experiment workers from previous runs."""
    _ensure_dirs()
    stopped = 0
    for pid_file in glob.glob(os.path.join(PID_DIR, "worker-exp-*.pid")):
        pid, meta = _read_pid_file(pid_file)
        if pid is not None and is_pid_alive(pid):
            stop_pid(pid, timeout=timeout)
            stopped += 1
        try:
            os.remove(pid_file)
        except Exception:
            pass
    return stopped


def get_experiment_worker_count(experiment_id: Optional[str] = None) -> int:
    """Returns the count of currently active experiment workers."""
    _ensure_dirs()
    active = 0
    for pid_file in glob.glob(os.path.join(PID_DIR, "worker-exp-*.pid")):
        pid, meta = _read_pid_file(pid_file)
        if pid is not None:
            if is_pid_alive(pid):
                if not experiment_id or meta.get("experiment_id") == experiment_id:
                    active += 1
            else:
                try:
                    os.remove(pid_file)
                except Exception:
                    pass
    return active


def start_experiment_workers(
    experiment_id: str,
    worker_count: int,
    exit_when_empty: bool = True,
    poll_interval: float = 0.1
) -> List[subprocess.Popen]:
    """
    Starts exactly `worker_count` independent worker processes targeting `experiment_id`.
    Validates that all processes are alive, aborting on any failure.
    """
    if worker_count not in [1, 2, 4, 8]:
        raise ValueError(f"Invalid worker count {worker_count}. Must be 1, 2, 4, or 8.")

    _ensure_dirs()
    stop_stale_experiment_workers()

    processes: List[subprocess.Popen] = []
    pids: List[int] = []

    try:
        for i in range(1, worker_count + 1):
            worker_id = f"exp-{experiment_id}-w{i}"
            pid_file = os.path.join(PID_DIR, f"worker-exp-{experiment_id}-w{i}.pid")
            log_file = os.path.join(LOG_DIR, f"worker-exp-{experiment_id}-w{i}.log")

            cmd = [
                sys.executable,
                os.path.join(BASE_DIR, "worker.py"),
                "--worker-id", worker_id,
                "--worker-mode", "experiment",
                "--experiment-id", experiment_id,
                "--mode", config.CLOUDQUEUE_MODE,
                "--poll-interval", str(poll_interval)
            ]
            if exit_when_empty:
                cmd.append("--exit-when-empty")

            p = _spawn_worker(cmd, log_file)
            processes.append(p)
            pids.append(p.pid)

            metadata = {
                "worker_mode": "experiment",
                "worker_id": worker_id,
                "experiment_id": experiment_id,
                "started_at": time.time(),
                "cmd": cmd
            }
            _write_pid_file(pid_file, p.pid, metadata)

        time.sleep(0.2)
        alive_pids = [pid for pid in pids if is_pid_alive(pid)]
        if len(alive_pids) != worker_count:
            raise RuntimeError(
                f"Experiment worker startup failed: requested {worker_count}, but only {len(alive_pids)} alive."
            )

        return processes

    except Exception as e:
        for pid in pids:
            stop_pid(pid, timeout=1.0)
        stop_stale_experiment_workers()
        raise e


def stop_experiment_workers(experiment_id: Optional[str] = None, timeout: float = 5.0) -> int:
    """Stops all experiment workers (optionally matching `experiment_id`)."""
    _ensure_dirs()
    stopped = 0
    for pid_file in glob.glob(os.path.join(PID_DIR, "worker-exp-*.pid")):
        pid, meta = _read_pid_file(pid_file)
        if experiment_id and meta.get("experiment_id") != experiment_id:
            continue

        if pid is not None and is_pid_alive(pid):
            stop_pid(pid, timeout=timeout)
            stopped += 1

        try:
            os.remove(pid_file)
        except Exception:
            pass

    return stopped


def count_active_workers() -> Dict[str, Any]:
    """Returns a breakdown of all currently active normal and experiment workers."""
    _ensure_dirs()
    normal_count = get_normal_worker_count()

    exp_counts: Dict[str, int] = {}
    for pid_file in glob.glob(os.path.join(PID_DIR, "worker-exp-*.pid")):
        pid, meta = _read_pid_file(pid_file)
        if pid is not None and is_pid_alive(pid):
            eid = meta.get("experiment_id", "unknown")
            exp_counts[eid] = exp_counts.get(eid, 0) + 1
        elif pid is not None:
            try:
                os.remove(pid_file)
            except Exception:
                pass

    total_exp = sum(exp_counts.values())
    return {
        "normal": normal_count,
        "experiment": total_exp,
        "experiment_breakdown": exp_counts,
        "total": normal_count + total_exp
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CloudQueue Worker Manager CLI")
    parser.add_argument("--status", action="store_true", help="Display active worker status")
    parser.add_argument("--start-normal", action="store_true", help="Start normal worker")
    parser.add_argument("--stop-normal", action="store_true", help="Stop normal worker")
    parser.add_argument("--stop-experiments", action="store_true", help="Stop all experiment workers")
    parser.add_argument("--clean-stale", action="store_true", help="Clean stale experiment workers")
    args = parser.parse_args()

    if args.status:
        print(json.dumps(count_active_workers(), indent=2))
    elif args.start_normal:
        pid = start_normal_worker()
        print(f"Normal worker started with PID {pid}")
    elif args.stop_normal:
        stop_normal_worker()
        print("Normal worker stopped.")
    elif args.stop_experiments:
        c = stop_experiment_workers()
        print(f"Stopped {c} experiment worker(s).")
    elif args.clean_stale:
        c = stop_stale_experiment_workers()
        print(f"Cleaned {c} stale worker(s).")
    else:
        print(json.dumps(count_active_workers(), indent=2))

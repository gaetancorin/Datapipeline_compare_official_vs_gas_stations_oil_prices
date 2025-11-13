import os
import glob
import sys
import signal

def acquire_lock(lockfile):
    """Acquire a lock on the specified file."""
    try:
        fd = os.open(lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        return fd
    except FileExistsError:
        return None

def release_lock(fd,lockfile):
    """Release the lock."""
    os.close(fd)
    os.remove(lockfile)


def cleanup_all_lockfiles():
    """Remove all .lock files in the current directory."""
    for file in glob.glob("*.lock"):
        try:
            os.remove(file)
            print(f"[INFO] Removed lock file: {file}")
        except Exception as e:
            print(f"[ERROR] Could not remove lock file {file}: {e}")

def handle_exit_signal(signum, frame):
    if signum == 2:
        print(f"[INFO] Detecting manual program interruption. Cleaning up lock files...")
    elif signum == 15:
        print(f"[INFO] Detecting termination signal (Docker stop). Cleaning up lock files...")
    else:
        print(f"[INFO] Signal {signum} received. Cleaning up lock files...")
    cleanup_all_lockfiles()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit_signal) # Ctrl+C
signal.signal(signal.SIGTERM, handle_exit_signal) # Docker Stop
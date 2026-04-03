import os
import sys
import time
import random
import datetime
import traceback

def log(msg):
    print(f"[INFO] {datetime.datetime.utcnow()} | {msg}", flush=True)

def err(msg):
    print(f"[ERROR] {datetime.datetime.utcnow()} | {msg}", file=sys.stderr, flush=True)

def main():
    log("Container started")
    log(f"Args: {sys.argv[1:]}")
    log(f"User: {os.getuid()}:{os.getgid()}")
    log(f"CWD: {os.getcwd()}")

    # simulate runtime
    sleep_time = int(os.getenv("TEST_SLEEP", random.randint(10, 40)))
    log(f"Sleeping for {sleep_time}s")
    time.sleep(sleep_time)

    # simulate writing output
    output_dir = "/app/outputs"
    if os.path.isdir(output_dir):
        fname = f"test_{random.randint(1000,9999)}.txt"
        path = os.path.join(output_dir, fname)
        try:
            with open(path, "a") as f:
                f.write(f"Test output at {datetime.datetime.utcnow()}\n")
            log(f"Wrote file {path}")
        except Exception as e:
            err(f"Failed writing output: {e}")
    else:
        err("Output directory not mounted")

    # simulate python warning / stderr
    if random.random() < 0.3:
        err("Simulated warning")

    # simulate python exception
    if random.random() < 0.3:
        try:
            raise ValueError("Simulated Python exception")
        except Exception:
            err("Exception occurred:")
            traceback.print_exc()
            sys.exit(1)

    log("Completed successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()
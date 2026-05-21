import os
import sys
import time
import random
import datetime
import traceback
from pathlib import Path
from functools import partial
import hydra
import logging

log: logging.Logger = logging.getLogger(__name__)


class UTCFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        ct = datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc)
        if datefmt:
            return ct.strftime(datefmt)
        return ct.strftime(self.default_time_format + ",%f")[:-3]


def patch_logging_utc():
    for handler in logging.root.handlers:
        current_fmt = handler.formatter._fmt if handler.formatter else '[%(asctime)s][%(name)s][%(levelname)s] - %(message)s'
        handler.setFormatter(
            # UTCFormatter(fmt="[%(levelname)s] %(asctime)s | %(name)s | %(message)s")
            UTCFormatter(fmt=current_fmt)
        )


def sleep_with_heartbeat(total_seconds, interval=5):
    log.info(f"Sleeping for {total_seconds}s (heartbeat every {interval}s)")
    elapsed = 0
    while elapsed < total_seconds:
        chunk = min(interval, total_seconds - elapsed)
        time.sleep(chunk)
        elapsed += chunk
        remaining = total_seconds - elapsed
        mem = get_mem_usage()
        log.info(f"Heartbeat | elapsed={elapsed}s | remaining={remaining}s | mem={mem}")


def get_mem_usage():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return line.strip()
    except Exception:
        pass
    return "unavailable"


@hydra.main(config_path="./conf", config_name="config.yaml")
def main(cfg):
    log_extra = logging.getLogger(name="extra")
    patch_logging_utc()
    log_extra.setLevel(logging.INFO)
    rootlog = logging.getLogger()
    extra_log_file = Path(
        getattr(rootlog.handlers[1], "baseFilename", "./")
    ).parent.joinpath("history.log")
    file_handler_extra = logging.FileHandler(extra_log_file)
    file_handler_extra.setFormatter(rootlog.handlers[0].formatter)
    log_extra.addHandler(file_handler_extra)

    def custom_exception_handler(exc_type, exc_value, exc_traceback):
        # Log the exception
        log.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

        # Call the default exception hook (prints the traceback and exits)
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = custom_exception_handler

    log.info("Container started")
    log.info(f"Args: {sys.argv[1:]}")
    log.info(f"User: {os.getuid()}:{os.getgid()}")
    log.info(f"CWD: {os.getcwd()}")
    log.info(f"Python: {sys.version}")
    log.info(f"PID: {os.getpid()}")
    log_extra.info("DOES this work?")

    # Log all env vars (optional: filter sensitive ones)
    for key, val in sorted(os.environ.items()):
        log.info(f"ENV | {key}={val}")

    sleep_time = int(os.getenv("TEST_SLEEP", random.randint(30, 90)))

    sleep_with_heartbeat(sleep_time, interval=5)

    # simulate writing output
    output_dir = "/app/outputs"
    log.info(f"Checking output directory: {output_dir}")
    if os.path.isdir(output_dir):
        fname = f"test_{random.randint(1000,9999)}.txt"
        path = os.path.join(output_dir, fname)
        try:
            with open(path, "a") as f:
                f.write(f"Test output at {datetime.datetime.now(datetime.UTC)}\n")
            log.info(f"Wrote file: {path}")
        except Exception as e:
            log.error(f"Failed writing output: {e}")
            traceback.print_exc()
    else:
        log.error(f"Output directory not mounted or missing: {output_dir}")

    # simulate python warning / stderr
    if random.random() < 0.3:
        log.warning("Simulated warning")

    # simulate python exception
    if random.random() < 0.3:
        try:
            raise ValueError("Simulated Python exception")
        except Exception:
            log.error("Exception occurred:")
            traceback.print_exc()
            sys.exit(1)

    log.info("Completed successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()

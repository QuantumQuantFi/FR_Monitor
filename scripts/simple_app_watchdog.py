#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _read_pid(path: Path) -> Optional[int]:
    try:
        txt = path.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    try:
        pid = int(txt)
    except Exception:
        return None
    return pid if pid > 0 else None


def _write_pid(path: Path, pid: int) -> None:
    path.write_text(f"{int(pid)}\n", encoding="utf-8")


def _port_open(host: str, port: int, timeout_s: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            return True
    except Exception:
        return False


def _rotate_log_if_needed(log_path: Path, *, max_bytes: int, keep: int, log_event) -> None:
    try:
        st = log_path.stat()
    except FileNotFoundError:
        return
    except Exception:
        return
    if st.st_size <= max_bytes:
        return
    ts = _utc_now().strftime("%Y%m%d_%H%M%S")
    rotated = log_path.with_name(f"{log_path.name}.{ts}")
    try:
        log_path.rename(rotated)
        log_event(f"log_rotated path={rotated} size_bytes={st.st_size}")
    except Exception as exc:
        log_event(f"log_rotate_failed error={type(exc).__name__}:{exc}")
        return

    # Cleanup older rotations.
    try:
        siblings = sorted(
            [p for p in log_path.parent.glob(f"{log_path.name}.*") if p.is_file()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for p in siblings[keep:]:
            try:
                p.unlink()
            except Exception:
                pass
    except Exception:
        pass


def _tail(path: Path, n: int = 80) -> str:
    try:
        out = subprocess.run(
            ["bash", "-lc", f"tail -n {int(n)} {sh_quote(str(path))} 2>/dev/null || true"],
            check=False,
            capture_output=True,
            text=True,
        )
        return (out.stdout or "").strip()
    except Exception:
        return ""


def _dmesg_oom_tail(n: int = 120) -> str:
    try:
        out = subprocess.run(
            ["bash", "-lc", f"dmesg --ctime 2>/dev/null | rg -n \"Killed process|Out of memory|oom-kill\" | tail -n {int(n)} || true"],
            check=False,
            capture_output=True,
            text=True,
        )
        return (out.stdout or "").strip()
    except Exception:
        return ""


def sh_quote(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"


def _describe_rc(rc: int) -> str:
    if rc >= 0:
        return f"exit_code={rc}"
    signum = -rc
    name = None
    try:
        name = signal.Signals(signum).name
    except Exception:
        name = None
    return f"signal={signum}" + (f"({name})" if name else "")


def main() -> int:
    parser = argparse.ArgumentParser(description="Keep simple_app.py running; restart on exit and log reasons.")
    parser.add_argument("--cmd", type=str, default="./venv/bin/python -u simple_app.py")
    parser.add_argument("--port", type=int, default=4002)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--startup-grace", type=float, default=25.0)
    parser.add_argument("--pid-file", type=str, default=str(REPO_ROOT / ".simple_app.pid"))
    parser.add_argument("--watchdog-pid-file", type=str, default=str(REPO_ROOT / ".simple_app_watchdog.pid"))
    parser.add_argument("--app-log", type=str, default=str(REPO_ROOT / "nohup_simple_app.out"))
    parser.add_argument("--watchdog-log", type=str, default=str(REPO_ROOT / "logs" / "simple_app_watchdog.log"))
    parser.add_argument("--rotate-mb", type=float, default=200.0)
    parser.add_argument("--rotate-keep", type=int, default=5)
    args = parser.parse_args()

    pid_path = Path(args.pid_file)
    watchdog_pid_path = Path(args.watchdog_pid_file)
    app_log_path = Path(args.app_log)
    watchdog_log_path = Path(args.watchdog_log)
    watchdog_log_path.parent.mkdir(parents=True, exist_ok=True)

    existing_wd = _read_pid(watchdog_pid_path)
    if existing_wd and _pid_exists(existing_wd):
        print(f"[{_iso_now()}] watchdog already running pid={existing_wd}", flush=True)
        return 0

    _write_pid(watchdog_pid_path, os.getpid())

    def log_event(msg: str) -> None:
        line = f"[{_iso_now()}] {msg}\n"
        try:
            watchdog_log_path.parent.mkdir(parents=True, exist_ok=True)
            with watchdog_log_path.open("a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass
        try:
            print(line.strip(), flush=True)
        except Exception:
            pass

    max_bytes = int(float(args.rotate_mb) * 1024 * 1024)

    # If simple_app is already running, we don't kill it; we just keep an eye on the port.
    last_seen_pid: Optional[int] = None
    pid0 = _read_pid(pid_path)
    if pid0 and _pid_exists(pid0):
        last_seen_pid = pid0
        log_event(f"adopt_existing_app pid={pid0}")

    backoff_s = 1.0
    while True:
        # If an external start happened, update last_seen_pid.
        pid_cur = _read_pid(pid_path)
        if pid_cur and _pid_exists(pid_cur):
            last_seen_pid = pid_cur

        port_ok = _port_open(str(args.host), int(args.port))
        if port_ok:
            time.sleep(float(args.interval))
            backoff_s = 1.0
            continue

        # At this point port is down. Record context and restart.
        if last_seen_pid and not _pid_exists(last_seen_pid):
            log_event(f"detected_app_stopped pid={last_seen_pid}")
        else:
            log_event(f"port_down host={args.host} port={args.port} pid={last_seen_pid or 'unknown'}")

        oom = _dmesg_oom_tail()
        if oom:
            log_event("dmesg_oom_tail_begin")
            for line in oom.splitlines():
                log_event(f"dmesg {line}")
            log_event("dmesg_oom_tail_end")

        tail = _tail(app_log_path, n=60)
        if tail:
            log_event("app_log_tail_begin")
            for line in tail.splitlines():
                log_event(f"app_log {line}")
            log_event("app_log_tail_end")

        _rotate_log_if_needed(app_log_path, max_bytes=max_bytes, keep=int(args.rotate_keep), log_event=log_event)

        # Spawn a managed process and wait; we get a return code for "why it stopped".
        log_event(f"starting cmd={args.cmd}")
        app_log_path.parent.mkdir(parents=True, exist_ok=True)
        with app_log_path.open("a", encoding="utf-8") as out:
            proc = subprocess.Popen(
                ["bash", "-lc", args.cmd],
                cwd=str(REPO_ROOT),
                stdout=out,
                stderr=out,
                start_new_session=True,
            )
        _write_pid(pid_path, proc.pid)
        last_seen_pid = proc.pid

        # Wait until port is up or the process exits.
        start_ts = time.time()
        while True:
            rc = proc.poll()
            if rc is not None:
                log_event(f"app_exited pid={proc.pid} {_describe_rc(int(rc))}")
                break
            if _port_open(str(args.host), int(args.port)):
                log_event(f"app_healthy pid={proc.pid} port={args.port}")
                backoff_s = 1.0
                break
            if (time.time() - start_ts) > float(args.startup_grace):
                log_event(f"startup_timeout pid={proc.pid} grace_s={args.startup_grace}; terminating")
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except Exception:
                    pass
                time.sleep(2.0)
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    pass
                break
            time.sleep(0.5)

        # If it exited quickly or failed to start, apply backoff.
        if not _port_open(str(args.host), int(args.port)):
            sleep_s = min(60.0, backoff_s)
            log_event(f"restart_backoff_s={sleep_s}")
            time.sleep(sleep_s)
            backoff_s = min(60.0, backoff_s * 2.0)


if __name__ == "__main__":
    raise SystemExit(main())


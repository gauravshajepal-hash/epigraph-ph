import argparse
import os
import re
import sqlite3
import subprocess
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

import yaml


DB_PATH = Path("data/epigraph.db")
LOG_PATH = Path("logs/production.full.log")
CONFIG_PATH = Path("config.yaml")
ACTIVE_STATUSES = ("parsing", "extracting", "verifying")
STATUS_ORDER = (
    "downloaded",
    "parsing",
    "parsed",
    "extracting",
    "extracted",
    "verifying",
    "verified",
    "synced",
    "failed",
)
TIMESTAMP_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}")
RESUME_RE = re.compile(r"Resuming (?P<name>.+) from page (?P<page>\d+)/(?P<total>\d+)")
START_RE = re.compile(r"Parsing (?P<name>.+) with checkpoints\.\.\.")
WORKER_PID_RE = re.compile(r"pipeline-(?P<pid>\d+)-")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_default_folder_filters() -> list[str]:
    if not CONFIG_PATH.exists():
        return []
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            cfg = yaml.safe_load(handle) or {}
    except Exception:
        return []

    folders = cfg.get("processing", {}).get("include_folders", []) or []
    return [str(folder).strip() for folder in folders if str(folder).strip()]


def build_scope_clause(folder_filters: list[str] | None) -> tuple[str, list[str]]:
    normalized = []
    for folder in folder_filters or []:
        lowered = folder.replace("/", "\\").strip("\\").lower()
        if lowered:
            normalized.append(lowered)

    if not normalized:
        return "", []

    clause = " AND (" + " OR ".join(
        "LOWER(REPLACE(local_path, '/', '\\')) LIKE ?"
        for _ in normalized
    ) + ")"
    params = [f"%\\{folder}\\%" for folder in normalized]
    return clause, params


def get_status_counts(folder_filters: list[str] | None = None) -> dict[str, int]:
    scope_clause, scope_params = build_scope_clause(folder_filters)
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT status, COUNT(*) AS count FROM documents WHERE 1=1 {scope_clause} GROUP BY status",
            scope_params,
        ).fetchall()
    return {row["status"]: row["count"] for row in rows}


def get_active_documents(folder_filters: list[str] | None = None) -> list[sqlite3.Row]:
    placeholders = ", ".join("?" for _ in ACTIVE_STATUSES)
    scope_clause, scope_params = build_scope_clause(folder_filters)
    with get_connection() as conn:
        return conn.execute(
            f"""
            SELECT
                id,
                status,
                local_path,
                parse_checkpoint_page,
                page_count,
                claimed_by,
                claimed_at
            FROM documents
            WHERE status IN ({placeholders})
              {scope_clause}
            ORDER BY
                CASE status
                    WHEN 'parsing' THEN 1
                    WHEN 'extracting' THEN 2
                    WHEN 'verifying' THEN 3
                    ELSE 4
                END,
                claimed_at ASC,
                id ASC
            """,
            [*ACTIVE_STATUSES, *scope_params],
        ).fetchall()


def parse_sqlite_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def format_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "n/a"

    total_seconds = int(round(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def load_log_baseline(doc_name: str) -> tuple[float, int] | None:
    if not LOG_PATH.exists():
        return None

    baseline: tuple[float, int] | None = None
    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                ts_match = TIMESTAMP_RE.match(line)
                if not ts_match:
                    continue

                timestamp = datetime.strptime(
                    ts_match.group("ts"),
                    "%Y-%m-%d %H:%M:%S",
                ).replace(tzinfo=timezone.utc)

                resume_match = RESUME_RE.search(line)
                if resume_match and resume_match.group("name") == doc_name:
                    baseline = (timestamp.timestamp(), int(resume_match.group("page")) - 1)
                    continue

                start_match = START_RE.search(line)
                if start_match and start_match.group("name") == doc_name:
                    baseline = (timestamp.timestamp(), 0)
    except OSError:
        return None

    return baseline


def update_history(
    rows: list[sqlite3.Row],
    history: dict[int, deque[tuple[float, int]]],
    baselines: dict[int, tuple[float, int] | None],
    rate_window_minutes: int,
) -> None:
    now_ts = time.time()
    cutoff_ts = now_ts - (rate_window_minutes * 60)
    active_ids = {int(row["id"]) for row in rows}

    for doc_id in list(history.keys()):
        if doc_id not in active_ids:
            history.pop(doc_id, None)
            baselines.pop(doc_id, None)

    for row in rows:
        doc_id = int(row["id"])
        if doc_id not in baselines:
            baselines[doc_id] = load_log_baseline(Path(row["local_path"]).name)

        bucket = history.setdefault(doc_id, deque(maxlen=240))
        pages_done = int(row["parse_checkpoint_page"] or 0)

        if not bucket or bucket[-1][1] != pages_done:
            bucket.append((now_ts, pages_done))
        else:
            bucket[-1] = (now_ts, pages_done)

        while bucket and bucket[0][0] < cutoff_ts:
            bucket.popleft()


def compute_rolling_ppm(samples: deque[tuple[float, int]]) -> float | None:
    if len(samples) < 2:
        return None

    start_ts, start_pages = samples[0]
    end_ts, end_pages = samples[-1]
    page_delta = end_pages - start_pages
    minute_delta = (end_ts - start_ts) / 60
    if page_delta <= 0 or minute_delta <= 0:
        return None
    return page_delta / minute_delta


def compute_average_ppm(
    baseline: tuple[float, int] | None,
    pages_done: int,
    now_ts: float,
) -> float | None:
    if not baseline:
        return None

    start_ts, start_pages = baseline
    page_delta = pages_done - start_pages
    minute_delta = (now_ts - start_ts) / 60
    if page_delta <= 0 or minute_delta <= 0:
        return None
    return page_delta / minute_delta


def classify_activity(last_update_seconds: float | None, slow_seconds: int, stall_seconds: int) -> str:
    if last_update_seconds is None:
        return "unknown"
    if last_update_seconds >= stall_seconds:
        return "POSSIBLE STALL"
    if last_update_seconds >= slow_seconds:
        return "slow page"
    return "active"


def extract_worker_pid(claimed_by: str) -> int | None:
    match = WORKER_PID_RE.search(claimed_by or "")
    if not match:
        return None
    return int(match.group("pid"))


def process_exists(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return False
    return str(pid) in result.stdout


def render_counts(status: dict[str, int], folder_filters: list[str] | None) -> list[str]:
    total = sum(status.values())
    parts = [f"Total={total}"]
    for key in STATUS_ORDER:
        parts.append(f"{key}={status.get(key, 0)}")
    scope_label = ", ".join(folder_filters or []) if folder_filters else "all-documents"
    return [
        "EpiGraph PH Pipeline Monitor",
        "===========================",
        f"Scope={scope_label}",
        " | ".join(parts),
        "",
    ]


def render_active_docs(
    rows: list[sqlite3.Row],
    history: dict[int, deque[tuple[float, int]]],
    baselines: dict[int, tuple[float, int] | None],
    slow_seconds: int,
    stall_seconds: int,
) -> list[str]:
    if not rows:
        return ["Active work: none", ""]

    lines = ["Active work:"]
    now_ts = time.time()
    now_utc = datetime.now(timezone.utc)

    for row in rows:
        doc_id = int(row["id"])
        path = Path(row["local_path"])
        status = row["status"]
        claimed_by = row["claimed_by"] or "-"
        worker_pid = extract_worker_pid(claimed_by)
        worker_alive = process_exists(worker_pid)
        claimed_at = parse_sqlite_timestamp(row["claimed_at"])
        last_update_seconds = (
            (now_utc - claimed_at).total_seconds() if claimed_at is not None else None
        )
        activity_state = (
            "worker missing"
            if worker_pid and not worker_alive
            else classify_activity(last_update_seconds, slow_seconds, stall_seconds)
        )

        if status == "parsing":
            done = int(row["parse_checkpoint_page"] or 0)
            total = int(row["page_count"] or 0)
            if total > 0:
                percent = (done / total) * 100
                progress = f"{done}/{total} pages ({percent:5.1f}%)"
            else:
                progress = f"{done} pages"

            rolling_ppm = compute_rolling_ppm(history.get(doc_id, deque()))
            average_ppm = compute_average_ppm(baselines.get(doc_id), done, now_ts)
            ppm = rolling_ppm if rolling_ppm is not None else average_ppm
            eta_seconds = None
            if ppm and total > 0 and done < total:
                eta_seconds = ((total - done) / ppm) * 60

            next_page = min(done + 1, total) if total > 0 else done + 1
            rate_text = f"{ppm:.2f} ppm" if ppm is not None else "warming up"
            lines.append(
                f"- [{status}] {path.name} | {progress} | next={next_page} | rate={rate_text} | eta={format_duration(eta_seconds)} | last update={format_duration(last_update_seconds)} ago | {activity_state} | worker={claimed_by}"
            )
        else:
            lines.append(
                f"- [{status}] {path.name} | last update={format_duration(last_update_seconds)} ago | {activity_state} | worker={claimed_by}"
            )

    lines.append("")
    return lines


def render_instructions(refresh_seconds: int, slow_seconds: int, stall_seconds: int) -> list[str]:
    return [
        f"Refreshing every {refresh_seconds}s. Press Ctrl+C to stop.",
        f"Slow page threshold={slow_seconds}s. Possible stall threshold={stall_seconds}s.",
        "Tip: use this monitor for live parse progress. `Get-Content logs\\production.full.log -Wait -Tail 30` is still useful for stage transitions and warnings.",
    ]


def clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor pipeline progress from SQLite.")
    parser.add_argument(
        "--refresh",
        type=int,
        default=5,
        help="Refresh interval in seconds (default: 5).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Print one snapshot and exit.",
    )
    parser.add_argument(
        "--slow-seconds",
        type=int,
        default=240,
        help="Mark the current page as slow if no checkpoint moves for this many seconds (default: 240).",
    )
    parser.add_argument(
        "--stall-seconds",
        type=int,
        default=900,
        help="Mark possible stall if no checkpoint moves for this many seconds (default: 900).",
    )
    parser.add_argument(
        "--rate-window-minutes",
        type=int,
        default=30,
        help="Rolling window for pages/minute calculation (default: 30).",
    )
    parser.add_argument(
        "--all-docs",
        action="store_true",
        help="Ignore config scope and monitor the full documents table.",
    )
    args = parser.parse_args()

    folder_filters = [] if args.all_docs else load_default_folder_filters()
    history: dict[int, deque[tuple[float, int]]] = defaultdict(lambda: deque(maxlen=240))
    baselines: dict[int, tuple[float, int] | None] = {}

    while True:
        status = get_status_counts(folder_filters=folder_filters)
        active_rows = get_active_documents(folder_filters=folder_filters)
        update_history(active_rows, history, baselines, args.rate_window_minutes)

        clear_screen()
        lines = []
        lines.extend(render_counts(status, folder_filters))
        lines.extend(
            render_active_docs(
                active_rows,
                history,
                baselines,
                args.slow_seconds,
                args.stall_seconds,
            )
        )
        lines.extend(
            render_instructions(
                args.refresh,
                args.slow_seconds,
                args.stall_seconds,
            )
        )
        print("\n".join(lines))

        if args.once:
            break

        try:
            time.sleep(args.refresh)
        except KeyboardInterrupt:
            print("\nMonitor stopped.")
            break


if __name__ == "__main__":
    main()

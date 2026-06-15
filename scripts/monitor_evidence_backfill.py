#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STATUS = Path("data/logs/evidence-backfill-status.json")
DEFAULT_DB = Path("data/wikiwar.db")


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor WikiWar historical evidence backfill progress.")
    parser.add_argument("--status-file", type=Path, default=DEFAULT_STATUS)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--interval", type=float, default=2.0, help="Refresh interval in seconds.")
    parser.add_argument("--once", action="store_true", help="Print one snapshot and exit.")
    args = parser.parse_args()

    try:
        while True:
            status = read_status(args.status_file)
            output = render(status, args.db)
            if args.once:
                print(output)
                return 0
            print("\033[2J\033[H" + output, end="", flush=True)
            time.sleep(max(args.interval, 0.25))
    except KeyboardInterrupt:
        print("\nStopped.")
        return 0


def read_status(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "status": "not_started",
            "phase": "not_started",
            "message": f"No status file at {path}",
        }
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {"status": "unreadable", "phase": "unreadable", "message": str(exc)}


def render(status: dict[str, Any], db_path: Path) -> str:
    width = max(40, min(shutil.get_terminal_size((100, 24)).columns, 140))
    bar_width = max(18, min(42, width - 46))

    lines = [
        "WikiWar Evidence Backfill",
        "=" * min(width, 80),
        f"Status: {status.get('status', 'unknown')}  Phase: {human_phase(status.get('phase'))}",
    ]
    if status.get("current_period"):
        lines.append(f"Period: {status['current_period']}")
    updated_age = status_age_seconds(status.get("updated_at"))
    if status.get("updated_at"):
        lines.append(f"Updated: {status['updated_at']} ({age(status['updated_at'])})")
    activity = worker_activity()
    if status.get("status") == "running" and updated_age is not None and updated_age > 120:
        lines.append("Warning: status file is stale; the worker may be inside one large XML page or blocked.")
    if activity:
        lines.append(
            "Worker: "
            f"{activity['workers']} evidence proc(s), {activity['decompressors']} decompressor proc(s), "
            f"CPU {activity['cpu']:.1f}%"
        )
    if status.get("message"):
        lines.append(f"Message: {status['message']}")

    lines.append("")
    lines.append(progress_line("Years", status.get("periods_done"), status.get("periods_total"), bar_width))

    if status.get("history_dumps_total"):
        detail = str(status.get("current_history_dump") or "")
        rows_seen = status.get("current_history_rows_seen")
        if rows_seen is not None:
            detail = f"{detail}  rows={int(rows_seen):,}".strip()
        lines.append(
            progress_line(
                "Talk dumps",
                status.get("history_dumps_done"),
                status.get("history_dumps_total"),
                bar_width,
                detail,
            )
        )

    if status.get("shards_total"):
        lines.append(progress_line("Downloads", status.get("shards_done"), status.get("shards_total"), bar_width))

    if status.get("parse_shards_total"):
        detail = str(status.get("current_parse_shard") or "")
        pages_seen = status.get("current_parse_pages_seen")
        revisions_seen = status.get("current_parse_revisions_seen")
        if pages_seen is not None:
            detail = f"{detail}  pages={int(pages_seen):,}".strip()
        if revisions_seen is not None:
            detail = f"{detail}  revisions={int(revisions_seen):,}".strip()
        if status.get("checkpoint_shards_reused"):
            detail = f"{detail}  reused={int(status['checkpoint_shards_reused']):,}".strip()
        if status.get("checkpoint_shards_written"):
            detail = f"{detail}  checkpointed={int(status['checkpoint_shards_written']):,}".strip()
        lines.append(
            progress_line(
                "XML parse",
                status.get("parse_shards_done"),
                status.get("parse_shards_total"),
                bar_width,
                detail,
            )
        )

    lines.append("")
    lines.append(f"Candidates: {fmt_int(status.get('candidates_total'))}")
    lines.append(f"Talk pages found: {fmt_int(status.get('talk_pages_found') or status.get('talk_pages_found_in_xml'))}")
    lines.append(f"Article pages found in XML: {fmt_int(status.get('article_pages_found'))}")
    lines.append(f"Pages written this phase: {fmt_int(status.get('pages_written'))}")
    lines.append(f"Evidence cache rows: {fmt_int(evidence_cache_count(db_path))}")
    if status.get("checkpoint_dir"):
        lines.append(f"Checkpoint dir: {status['checkpoint_dir']}")

    if status.get("results"):
        lines.append("")
        lines.append("Completed periods:")
        for result in status["results"][-8:]:
            period = result.get("period", "?")
            pages = result.get("pages_written", 0)
            revisions = result.get("pages_with_article_revisions", 0)
            lines.append(f"  {period}: {pages} cached, {revisions} with article revisions")

    lines.append("")
    lines.append("Ctrl-C to stop. Use --once for a single snapshot.")
    return "\n".join(lines) + "\n"


def progress_line(label: str, done: Any, total: Any, bar_width: int, detail: str = "") -> str:
    done_i = safe_int(done)
    total_i = safe_int(total)
    if total_i <= 0:
        return f"{label:<12} [{'?' * min(bar_width, 3):<{bar_width}}] n/a"
    ratio = min(max(done_i / total_i, 0.0), 1.0)
    filled = int(round(ratio * bar_width))
    bar = "#" * filled + "-" * (bar_width - filled)
    suffix = f"{done_i:,}/{total_i:,} ({ratio * 100:5.1f}%)"
    if detail:
        suffix += f"  {detail}"
    return f"{label:<12} [{bar}] {suffix}"


def evidence_cache_count(db_path: Path) -> int | None:
    if not db_path.exists():
        return None


def worker_activity() -> dict[str, float | int] | None:
    try:
        output = subprocess.check_output(
            ["ps", "-axo", "pcpu,command"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    workers = 0
    decompressors = 0
    cpu = 0.0
    for line in output.splitlines()[1:]:
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split(None, 1)
        if len(parts) != 2:
            continue
        try:
            line_cpu = float(parts[0])
        except ValueError:
            continue
        command = parts[1]
        is_wrapper = command.startswith(("SCREEN ", "login ", "/bin/zsh ", "zsh "))
        is_worker = " -m wikiwar.evidence auto-backfill" in command and not is_wrapper
        is_decompressor = (
            "7z x -so data/evidence-dumps/" in command
            or "p7zip/7z x -so data/evidence-dumps/" in command
        )
        if is_worker:
            workers += 1
            cpu += line_cpu
        elif is_decompressor:
            decompressors += 1
            cpu += line_cpu
    if workers == 0 and decompressors == 0:
        return None
    return {"workers": workers, "decompressors": decompressors, "cpu": cpu}
    try:
        with sqlite3.connect(db_path) as connection:
            row = connection.execute("select count(*) from historical_evidence_cache").fetchone()
            return int(row[0]) if row else 0
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            return "locked"
        return None
    except sqlite3.Error:
        return None


def human_phase(value: Any) -> str:
    labels = {
        "not_started": "Not started",
        "starting": "Starting",
        "resolving_candidates": "Resolving candidates",
        "resolving_talk_pages": "Resolving talk pages",
        "downloading_shards": "Downloading XML shards",
        "parsing_shards": "Parsing XML shards",
        "writing_cache": "Writing evidence cache",
        "period_done": "Period complete",
        "complete": "Complete",
        "stopped": "Stopped",
        "unreadable": "Unreadable status",
    }
    return labels.get(str(value or ""), str(value or "unknown"))


def age(value: str) -> str:
    seconds = status_age_seconds(value)
    if seconds is None:
        return "unknown age"
    if seconds < 60:
        return f"{seconds}s ago"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s ago"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m ago"


def status_age_seconds(value: Any) -> int | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0, int((datetime.now(timezone.utc) - parsed).total_seconds()))


def fmt_int(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

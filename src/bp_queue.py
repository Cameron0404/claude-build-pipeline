#!/usr/bin/env python3
"""bp_queue.py - the timed work queue the pipeline's stages wait in.

A queue entry is a markdown file in `<home>/queue/pending/` with flat `key: value` frontmatter
and a body. Two kinds:

  kind: brief   the body is an operating brief for a fresh headless Claude Code session
  kind: resume  continue a run that was interrupted (network loss, usage limit) in place

`run_at` says when the entry becomes due, in the configured zone. An entry with no run_at is
due at the next 03:30 after it was written ("tonight"). The runner (`bp_runner.py tick`) moves a
due entry to `done/` as `<stamp>_<name>.md` when it launches it, so `done/` is the launch record.

`requires:` holds gates the runner checks before it launches a brief, separated by ", ":

  file:<path>                     the file exists (the pipeline's gate files use this)
  branch-merged:<repo>:<branch>   the branch is an ancestor of the repo's main
  queue-done:<entry name>         that entry was launched AND its run finished `done`

A gate that is not met defers the entry 30 minutes; it never fails the queue.

    bp_queue.py add --at tonight --title "..." --file brief.md   # or --text; --at now|HH:MM|+2h|ISO
    bp_queue.py list
    bp_queue.py now FILE          # due now; gates still hold
    bp_queue.py due               # print due files, exit 3 if none

Stdlib only. Python 3.9 or later.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bp_config as C  # noqa: E402

P = C.TZ
QUEUE = C.QUEUE
PENDING = QUEUE / "pending"
DONE = QUEUE / "done"
RUNS = C.RUNS
DEFAULT_HOUR = (3, 30)
NOTHING_DUE = 3
OPUS = C.MODEL
FINISHED_NOT_DONE = ("failed", "dead", "timed-out", "killed", "orphaned")


def _now() -> dt.datetime:
    return dt.datetime.now(P)


def _slug(text: str, n: int = 48) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:n].rstrip("-") or "work"


def next_default(after: dt.datetime) -> dt.datetime:
    h, m = DEFAULT_HOUR
    cand = after.replace(hour=h, minute=m, second=0, microsecond=0)
    if cand <= after:
        cand += dt.timedelta(days=1)
    return cand


def parse_at(text: str, now: dt.datetime | None = None) -> dt.datetime:
    """'03:30' (next occurrence), '2026-09-10T03:30', '+2h', '+90m', 'tonight' (next 03:30), 'now'."""
    now = now or _now()
    t = text.strip().lower()
    if t in ("tonight", "overnight", "default"):
        return next_default(now)
    if t == "now":
        return now
    m = re.fullmatch(r"\+(\d+)\s*([hm])", t)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        return now + (dt.timedelta(hours=n) if unit == "h" else dt.timedelta(minutes=n))
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", t)
    if m:
        cand = now.replace(hour=int(m.group(1)), minute=int(m.group(2)), second=0, microsecond=0)
        if cand <= now:
            cand += dt.timedelta(days=1)
        return cand
    d = dt.datetime.fromisoformat(text.strip().replace(" ", "T"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=P)
    return d.astimezone(P)


def _split(text: str) -> tuple[dict, str]:
    """(flat frontmatter, body). The writer below only ever writes `key: value` lines."""
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---", 4)
    if end < 0:
        return {}, text
    fm = {}
    for line in text[4:end].splitlines():
        k, sep, v = line.partition(":")
        if sep and k.strip():
            fm[k.strip()] = v.strip().strip("\"'")
    body = text[end + 4:]
    return fm, body.lstrip("\n")


def _int(value, fallback: int) -> int:
    """A frontmatter number, or the fallback: one damaged entry must not stop the queue."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return fallback


def entry(path: Path) -> dict:
    fm, body = _split(path.read_text(encoding="utf-8", errors="replace"))
    mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, P)
    try:
        run_at = parse_at(fm["run_at"]) if fm.get("run_at") else next_default(mtime)
    except ValueError:
        run_at = next_default(mtime)
    return {
        "path": path, "fm": fm, "body": body,
        "title": fm.get("title") or (body.splitlines()[0].lstrip("# ").strip() if body.strip() else path.stem),
        "kind": fm.get("kind", "brief"), "model": fm.get("model", ""), "agent": fm.get("agent", ""),
        "run_at": run_at, "reason": fm.get("reason", ""), "requires": fm.get("requires", ""),
        "max_minutes": fm.get("max_minutes", ""),
    }


def _write(path: Path, fm: dict, body: str) -> Path:
    PENDING.mkdir(parents=True, exist_ok=True)
    DONE.mkdir(parents=True, exist_ok=True)
    lines = ["---"] + [f"{k}: {v}" for k, v in fm.items() if v not in ("", None)] + ["---", ""]
    path.write_text("\n".join(lines) + body.rstrip("\n") + "\n", encoding="utf-8")
    return path


def requires_items(fm: dict) -> list[str]:
    return [x.strip() for x in (fm.get("requires") or "").strip("[]").split(",") if x.strip()]


def queue_run_state(name: str) -> dict:
    """What happened to entry `name`: launched, its run status, and whether the runner gave up."""
    names = [p.name for p in DONE.glob("*.md")]
    launched = any(n.endswith((f"_{name}.md", f"_{name}-already-launched.md")) for n in names)
    gave_up = any(n.endswith(f"_{name}-gave-up-refused.md") for n in names)
    pending = (PENDING / f"{name}.md").exists()
    try:
        status = json.loads((RUNS / name / "state.json").read_text()).get("status") or ""
    except (OSError, ValueError):
        status = "done" if launched else ""
    if gave_up and not launched and status == "":
        status = "gave-up-refused"
    return {"launched": launched, "gave_up": gave_up, "pending": pending, "status": status}


def requires_status(fm: dict) -> list[dict]:
    """One dict per requirement: item, kind, met, why ("" when met), droppable."""
    out = []
    for item in requires_items(fm):
        kind, _, rest = item.partition(":")
        why, droppable = "", False
        if kind == "file":
            if not Path(rest.strip()).expanduser().exists():
                why = f"file {rest.strip()} does not exist yet"
        elif kind == "branch-merged":
            repo, _, branch = rest.rpartition(":")
            r = Path(repo).expanduser()
            try:
                have = subprocess.run(["git", "-C", str(r), "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
                                      capture_output=True, timeout=30).returncode == 0
                merged = have and subprocess.run(["git", "-C", str(r), "merge-base", "--is-ancestor", branch, "main"],
                                                 capture_output=True, timeout=30).returncode == 0
            except (OSError, subprocess.SubprocessError):
                have = merged = False
            if not merged:
                why = f"branch {branch} is not merged into main of {r}" if have else f"branch {branch} not found in {r}"
        elif kind == "queue-done":
            name = rest.strip()
            s = queue_run_state(name)
            if not (s["launched"] and s["status"] == "done"):
                if s["status"] == "gave-up-refused":
                    why, droppable = f"queue entry {name} gave up, it will never run by itself", True
                elif s["status"] in FINISHED_NOT_DONE:
                    why, droppable = f"queue entry {name} finished {s['status']}, not done", True
                else:
                    why = f"queue entry {name} is not done (run status: {s['status'] or 'not launched'})"
        else:
            why = f"unknown requirement {item!r}"
        out.append({"item": item, "kind": kind, "met": not why, "why": why, "droppable": droppable})
    return out


def unmet_requires(fm: dict) -> list[str]:
    return [r["why"] for r in requires_status(fm) if not r["met"]]


def run_now(path: Path, anyway: bool = False, now: dt.datetime | None = None) -> dict:
    """The entry's run_at becomes now, so the next tick picks it up. Never bypasses `requires`:
    `anyway` drops only requirements on runs that already finished without `done`."""
    now = now or _now()
    e = entry(path)
    st = requires_status(e["fm"])
    dropped = [r["item"] for r in st if anyway and not r["met"] and r["droppable"]]
    keep = [r["item"] for r in st if r["item"] not in dropped]
    fm = dict(e["fm"])
    fm.update({"title": e["title"], "kind": e["kind"], "run_at": now.strftime("%Y-%m-%dT%H:%M"),
               "requires": ", ".join(keep), "reason": "run now" + (f", dropped {len(dropped)}" if dropped else "")})
    _write(path, fm, e["body"])
    unmet = [r for r in st if not r["met"] and r["item"] not in dropped]
    return {"ok": True, "file": path.name, "dropped": dropped, "waiting_on": [r["why"] for r in unmet],
            "ready": not unmet}


def add_brief(title: str, body: str, run_at: dt.datetime, model: str = "", reason: str = "",
              max_minutes: int = 0, requires: str = "", name_at: dt.datetime | None = None) -> Path:
    """Queue a brief. The file name, and so the run id, carries `name_at` when given (the time it
    was queued), so a stage parked a week out is still named for the day it was queued."""
    stamp = name_at or run_at
    for q in PENDING.glob(f"{stamp:%Y-%m-%d}-*.md"):     # same title, same day, still pending: one is enough
        e = entry(q)
        if e["kind"] == "brief" and e["title"].strip().lower() == title.strip().lower():
            print(f"bp_queue: a brief titled {title!r} is already pending: {q.name}")
            return q
    name = f"{stamp:%Y-%m-%d-%H%M}-{_slug(title)}.md"
    fm = {"title": title, "kind": "brief", "run_at": run_at.strftime("%Y-%m-%dT%H:%M"),
          "model": model or OPUS, "queued": _now().strftime("%Y-%m-%dT%H:%M"),
          "reason": reason, "max_minutes": str(max_minutes) if max_minutes else "", "requires": requires}
    return _write(PENDING / name, fm, body)


def add_resume(agent_id: str, run_at: dt.datetime, reason: str = "") -> Path:
    """Queue a resume of run `agent_id` for run_at (an interrupted run continues in place)."""
    now = _now()
    for p in PENDING.glob("*.md"):
        e = entry(p)
        if e["kind"] == "resume" and e["fm"].get("agent") == agent_id and e["run_at"] > now:
            return p
    name = f"{run_at:%Y-%m-%d-%H%M}-resume-{_slug(agent_id)}.md"
    fm = {"title": f"Resume run {agent_id}", "kind": "resume", "agent": agent_id,
          "run_at": run_at.strftime("%Y-%m-%dT%H:%M"), "queued": now.strftime("%Y-%m-%dT%H:%M"), "reason": reason}
    return _write(PENDING / name, fm, f"Resume run {agent_id}. {reason}\n")


def defer(path: Path, minutes: int, reason: str = "") -> Path:
    """Keep an entry pending with run_at moved ahead. `due_since` keeps its first due time, so the
    entry that has waited longest takes the next free slot."""
    e = entry(path)
    fm = dict(e["fm"])
    fm.update({"title": e["title"], "kind": e["kind"],
               "run_at": (_now() + dt.timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M"),
               "reason": reason or e["reason"]})
    fm.setdefault("due_since", min(e["run_at"], _now()).strftime("%Y-%m-%dT%H:%M"))
    return _write(path, fm, e["body"])


def _due_since(e: dict) -> dt.datetime:
    try:
        return min(parse_at(e["fm"]["due_since"]), e["run_at"]) if e["fm"].get("due_since") else e["run_at"]
    except ValueError:
        return e["run_at"]


def due(now: dt.datetime | None = None) -> list[Path]:
    """Due entries, longest waiting first. A damaged entry is named on stderr and skipped."""
    now = now or _now()
    out = []
    for p in PENDING.glob("*.md"):
        try:
            e = entry(p)
            out.append((e["run_at"], _due_since(e), p))
        except (OSError, ValueError) as err:
            print(f"bp_queue: skipped damaged entry {p.name}: {err}", file=sys.stderr)
    return [p for r, since, p in sorted(out, key=lambda x: (x[1], x[0], x[2].name)) if r <= now]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("add")
    a.add_argument("--at", default="tonight")
    a.add_argument("--title", required=True)
    a.add_argument("--file")
    a.add_argument("--text")
    a.add_argument("--model", default="")
    a.add_argument("--max-minutes", type=int, default=0)
    a.add_argument("--requires", default="")
    sub.add_parser("list")
    n = sub.add_parser("now")
    n.add_argument("file")
    n.add_argument("--anyway", action="store_true")
    sub.add_parser("due")
    args = ap.parse_args()
    if args.cmd == "add":
        body = Path(args.file).read_text(encoding="utf-8") if args.file else (args.text or sys.stdin.read())
        print(add_brief(args.title, body, parse_at(args.at), args.model, "added by hand", args.max_minutes, args.requires))
        return 0
    if args.cmd == "list":
        for p in sorted(PENDING.glob("*.md")):
            e = entry(p)
            gate = f"  waits on: {'; '.join(unmet_requires(e['fm']))}" if e["requires"] else ""
            print(f"{e['run_at']:%a %d %b %H:%M}  {e['kind']:6}  {p.stem}{gate}")
        return 0
    if args.cmd == "now":
        f = Path(args.file)
        f = f if f.exists() else PENDING / (args.file if args.file.endswith(".md") else args.file + ".md")
        print(json.dumps(run_now(f, args.anyway)))
        return 0
    d = due()
    for p in d:
        print(p)
    return 0 if d else NOTHING_DUE


if __name__ == "__main__":
    sys.exit(main())

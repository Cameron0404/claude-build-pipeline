#!/usr/bin/env python3
"""bp_runner.py - the default runner: launches queued briefs as headless `claude -p` runs and
records what happened to each, in the files the supervisor reads.

    bp_runner.py tick              # launch what is due (gates met, under the run cap), reap dead runs
    bp_runner.py status            # every run, newest first
    bp_runner.py kill RUN_ID       # stop a run; its stage is retried by the supervisor
    bp_runner.py usage < status.json   # record a usage reading (pipe your status line's JSON in)
    bp_runner.py manage RUN_ID     # internal: the per-run manager process that tick starts

THE RUNNER INTERFACE. `build_pipeline.py` imports the module named by BUILD_PIPELINE_RUNNER
(this one by default) and uses only these names, so another runner (a fuller agent manager such
as Steward, https://github.com/Cameron0404/steward) can stand in by providing them:

    RUNS, USAGE, BUILD_LOCKS            paths (the tests point them at a temporary directory)
    BRIEF_REPO_RE                       reads `**Repo: <path>**` from line 1 of a brief
    DEFAULT_THRESHOLD, WEEKLY_WINDDOWN  usage percentages at which new stages are held
    pid_state(pid) -> "alive" | "dead" | "unknown"
    run_liveness(state, run_dir) -> (verdict, why)
    usage_now() -> {"pct", "resets_at", "week_pct", "week_resets_at", "fresh"}
    release_stage_locks(run_id) -> int

and by writing, for each queue entry it launches, `<runs>/<entry name>/` holding `state.json`
(`status`, `started`, `ended`, `reason`, `manager_pid`, `claude_pid`), `stream.jsonl` (the CLI's
stream-json events, the last `result` event included) and `progress.md` (the run's checkpoint,
which the brief tells the session to keep).

Statuses: starting, running, done, failed, timed-out, killed, interrupted (network or API error,
a resume is queued), limited (usage limit hit, a resume is queued for the reset), orphaned
(written only on a positive dead reading of both the manager and the session).

Stdlib only. Python 3.9 or later.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import atomicio  # noqa: E402
import bp_config as C  # noqa: E402
import bp_queue as Q  # noqa: E402

RUNS = C.RUNS
USAGE = C.STATE / "usage.json"
BUILD_LOCKS = C.BUILDS
DEFAULT_THRESHOLD = C.THRESHOLD
WEEKLY_WINDDOWN = C.WEEKLY
USAGE_STALE_S = 45 * 60          # an older reading cannot justify holding a stage
LIVE_FRESH_MIN = 10              # log.md or progress.md written this recently means alive
DEFAULT_MAX_MINUTES = 90
MAX_RESUMES = 3
NETWORK_RETRY_MIN = 10

BRIEF_REPO_RE = re.compile(r"^\*\*Repo:\s*`?([^`*\n]+?)`?\s*\*\*\s*$", re.M)
AUTH_RE = re.compile(r"not logged in|please run /login|invalid api key", re.I)
LIMIT_RE = re.compile(r"hit your (?:session|usage) limit|usage limit reached|out of usage credits", re.I)
NETWORK_RE = re.compile(r"can't reach the API server|ENOTFOUND|ECONNRESET|ECONNREFUSED|ETIMEDOUT|EAI_AGAIN|"
                        r"fetch failed|network error|overloaded|api_error", re.I)
LIVE = ("starting", "running")


def _now() -> dt.datetime:
    return dt.datetime.now(Q.P)


def _iso(t: dt.datetime) -> str:
    return t.isoformat(timespec="seconds")


def _parse(s: str):
    if not s:
        return None
    try:
        t = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=Q.P)


# ------------------------------------------------------------------------------ run state

def run_dir(rid: str) -> Path:
    return RUNS / rid


def read_state(rid: str) -> dict:
    try:
        return json.loads((run_dir(rid) / "state.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def write_state(rid: str, **changes) -> dict:
    st = read_state(rid)
    st.update(changes)
    st["id"] = rid
    st["updated"] = _iso(_now())
    d = run_dir(rid)
    d.mkdir(parents=True, exist_ok=True)
    atomicio.write_text(d / "state.json", json.dumps(st, indent=1, ensure_ascii=False) + "\n")
    return st


def log(rid: str, msg: str) -> None:
    d = run_dir(rid)
    d.mkdir(parents=True, exist_ok=True)
    with (d / "log.md").open("a", encoding="utf-8") as fh:
        fh.write(f"{_now():%Y-%m-%d %H:%M:%S} MANAGER: {msg}\n")


# ------------------------------------------------------------------------------ liveness

def pid_state(pid) -> str:
    """`alive`, `dead` or `unknown` for one pid, from os.kill(pid, 0) alone.

    Under a sandbox os.kill on a process outside the caller's tree raises PermissionError: the
    process exists and we may not signal it, so that is alive. Only ProcessLookupError is dead.
    Reading every error as dead once marked live runs orphaned and started second attempts
    into worktrees still in use (docs/failure-record.md, failure 11)."""
    try:
        n = int(pid)
    except (TypeError, ValueError):
        return "unknown"
    if n <= 0:
        return "unknown"
    try:
        os.kill(n, 0)
        return "alive"
    except ProcessLookupError:
        return "dead"
    except PermissionError:
        return "alive"
    except OSError:
        return "unknown"


def _fresh_file(rdir: Path, minutes: float):
    best = None
    for name in ("log.md", "progress.md"):
        try:
            age = (time.time() - (Path(rdir) / name).stat().st_mtime) / 60
        except OSError:
            continue
        if age <= minutes and (best is None or age < best[1]):
            best = (name, max(age, 0.0))
    return best


def run_liveness(st: dict, rdir: Path | None = None, fresh_min: float | None = None) -> tuple[str, str]:
    """(`alive` | `dead` | `unknown`, why), from the manager's pid, the session's pid and whether
    log.md or progress.md was written in the last LIVE_FRESH_MIN minutes. Any live signal is
    alive. Dead needs both pids positively dead (or no session pid ever recorded) and no fresh
    file. Anything else is unknown, and callers leave the state alone."""
    rdir = Path(rdir) if rdir else run_dir(st.get("id") or "")
    fresh_min = LIVE_FRESH_MIN if fresh_min is None else fresh_min
    mpid, cpid = st.get("manager_pid"), st.get("claude_pid")
    mgr = pid_state(mpid)
    ses = pid_state(cpid) if cpid else "none"
    if mgr == "alive":
        return "alive", f"manager pid {mpid} alive"
    if ses == "alive":
        return "alive", f"session pid {cpid} alive, manager pid {mpid or '?'} {mgr}"
    fresh = _fresh_file(rdir, fresh_min)
    if fresh:
        return "alive", f"{fresh[0]} written {fresh[1]:.0f} min ago"
    if mgr == "dead" and ses in ("dead", "none"):
        return "dead", f"manager pid {mpid} dead" + (f", session pid {cpid} dead" if cpid else ", no session pid recorded")
    if not mpid and not cpid and st.get("status") == "starting":
        return "dead", "no manager or session pid was ever recorded"
    return "unknown", f"manager pid {mpid or 'none'} {mgr}, session pid {cpid or 'none'} {ses}"


def release_stage_locks(rid: str) -> int:
    """Remove every build worktree lock held for run `rid`, so the stage's next attempt need not
    wait for the supervisor to find the lock stale. Returns how many were removed."""
    n = 0
    for f in Path(BUILD_LOCKS).glob("*/worktree.lock"):
        try:
            if json.loads(f.read_text(encoding="utf-8")).get("entry") == rid:
                f.unlink()
                n += 1
        except (OSError, ValueError):
            continue
    return n


# ------------------------------------------------------------------------------ usage

def _dig(d, path: str):
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _when(raw):
    if isinstance(raw, (int, float)):
        return dt.datetime.fromtimestamp(raw if raw < 1e12 else raw / 1000, Q.P)
    return _parse(str(raw)) if raw else None


def usage_now() -> dict:
    """The last usage reading: {pct, resets_at, week_pct, week_resets_at, age_s, fresh}.

    The file holds `{"_written": ISO, "data": {"rate_limits": {"five_hour": {"used_percentage",
    "resets_at"}, "seven_day": {...}}}}`, the shape `bp_runner.py usage` writes from a status
    line's JSON. No file, a corrupt one or one older than 45 minutes is not fresh, and a reading
    that is not fresh holds nothing."""
    blank = {"pct": None, "resets_at": None, "week_pct": None, "week_resets_at": None, "age_s": None, "fresh": False}
    try:
        raw = json.loads(Path(USAGE).read_text())
    except (OSError, ValueError):
        return blank
    written = _parse(raw.get("_written", ""))
    age = (_now() - written).total_seconds() if written else None
    data = raw.get("data", raw)
    lim = "rate_limits."

    def num(path):
        v = _dig(data, path)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    pct = num(lim + "five_hour.used_percentage")
    return {"pct": pct, "resets_at": _when(_dig(data, lim + "five_hour.resets_at")),
            "week_pct": num(lim + "seven_day.used_percentage"),
            "week_resets_at": _when(_dig(data, lim + "seven_day.resets_at")),
            "age_s": age, "fresh": age is not None and age < USAGE_STALE_S and pct is not None}


def record_usage(status_json: str) -> None:
    """Store a status line's JSON (it carries `rate_limits`) with the time it was read."""
    data = json.loads(status_json)
    Path(USAGE).parent.mkdir(parents=True, exist_ok=True)
    atomicio.write_text(USAGE, json.dumps({"_written": _iso(_now()), "data": data}) + "\n")


def _reset_time(default_min: int = 60) -> dt.datetime:
    u = usage_now()
    t = u.get("resets_at")
    return (t if t and t > _now() else _now() + dt.timedelta(minutes=default_min)) + dt.timedelta(minutes=2)


# ------------------------------------------------------------------------------ the manager

def _manager_note(rid: str) -> str:
    return (f"\n\n---\nManager note: you are a headless run with id `{rid}`. Your checkpoint file is "
            f"`{run_dir(rid) / 'progress.md'}`: keep it current as the brief says. Stay in the foreground "
            "and end your turn only when the work is finished.\n")


def _claude_cmd(model: str, session: str = "") -> list[str]:
    cmd = [C.CLAUDE, "-p", "--output-format", "stream-json", "--verbose"]
    if model:
        cmd += ["--model", model]
    cmd += shlex.split(C.CLAUDE_ARGS)
    if session:
        cmd += ["--resume", session]
    return cmd


def _done_file(rid: str) -> Path | None:
    files = sorted(Q.DONE.glob(f"*_{rid}.md"))
    return files[-1] if files else None


def manage(rid: str, resume: bool = False) -> str:
    """Run one session to its end and record the outcome. Returns the final status."""
    st = read_state(rid)
    f = _done_file(rid)
    if f is None:
        write_state(rid, status="failed", reason="no launched queue entry for this run", ended=_iso(_now()))
        return "failed"
    e = Q.entry(f)
    body = e["body"]
    m = BRIEF_REPO_RE.search(body.split("\n", 3)[0] if body else "")
    repo = Path(m.group(1).strip()).expanduser() if m else Path.home()
    if not repo.is_dir():
        write_state(rid, status="failed", reason=f"the brief's repo {repo} is not a directory", ended=_iso(_now()))
        release_stage_locks(rid)
        return "failed"
    max_min = Q._int(e.get("max_minutes"), DEFAULT_MAX_MINUTES) or DEFAULT_MAX_MINUTES
    session = st.get("session_id", "") if resume else ""
    prompt = ("Continue from your checkpoint and finish the brief. Stay in the foreground."
              + _manager_note(rid)) if session else body + _manager_note(rid)
    now = _iso(_now())
    write_state(rid, status="running", started=st.get("started") or now, started_this_launch=now,
                manager_pid=os.getpid(), model=e["model"], repo=str(repo), max_minutes=max_min, ended="")
    log(rid, ("resumed" if session else "launched") + f" in {repo}, cap {max_min} min")
    try:
        proc = subprocess.Popen(_claude_cmd(e["model"], session), cwd=str(repo), stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=_child_env())
    except OSError as err:
        write_state(rid, status="failed", reason=f"could not start {C.CLAUDE}: {err}", ended=_iso(_now()))
        release_stage_locks(rid)
        return "failed"
    write_state(rid, claude_pid=proc.pid)
    events = {"session": session, "result": None}
    err_tail: list[str] = []

    def pump_out():
        with (run_dir(rid) / "stream.jsonl").open("a", encoding="utf-8") as out:
            for line in proc.stdout:
                out.write(line)
                out.flush()
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                if ev.get("session_id") and not events["session"]:
                    events["session"] = ev["session_id"]
                    write_state(rid, session_id=ev["session_id"])
                if ev.get("type") == "result":
                    events["result"] = ev

    def pump_err():
        for line in proc.stderr:
            err_tail.append(line)
            del err_tail[:-40]

    threads = [threading.Thread(target=pump_out, daemon=True), threading.Thread(target=pump_err, daemon=True)]
    for t in threads:
        t.start()
    proc.stdin.write(prompt)
    proc.stdin.close()
    timed_out = False
    try:
        proc.wait(timeout=max_min * 60)
    except subprocess.TimeoutExpired:
        timed_out = True
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    for t in threads:
        t.join(timeout=10)
    res = events["result"] or {}
    text = " ".join([str(res.get("result") or ""), "".join(err_tail)])
    resumes = int(st.get("resumes", 0))
    if read_state(rid).get("status") == "killed":
        status, reason = "killed", "stopped by hand"
    elif timed_out:
        status, reason = "timed-out", f"ran past its {max_min}-minute cap"
    elif AUTH_RE.search(text):
        status, reason = "failed", "Not logged in: run `claude` once interactively and log in"
    elif LIMIT_RE.search(text) and events["session"]:
        when = _reset_time()
        status, reason = "limited", f"usage limit; resume queued for {when:%H:%M}"
        Q.add_resume(rid, when, "usage limit")
    elif proc.returncode != 0 and NETWORK_RE.search(text) and events["session"] and resumes < MAX_RESUMES:
        when = _now() + dt.timedelta(minutes=NETWORK_RETRY_MIN)
        status, reason = "interrupted", f"network or API error; resume queued for {when:%H:%M}"
        Q.add_resume(rid, when, "network or API error")
    elif proc.returncode == 0 and not res.get("is_error"):
        status, reason = "done", "finished"
    else:
        status, reason = "failed", f"exit {proc.returncode}: {text.strip()[-300:]}"
    write_state(rid, status=status, reason=reason, ended=_iso(_now()), exit_code=proc.returncode,
                resumes=resumes + (1 if status in ("interrupted", "limited") else 0),
                cost_usd=res.get("total_cost_usd"))
    log(rid, f"outcome {status}: {reason}")
    release_stage_locks(rid)
    return status


def _child_env() -> dict:
    """A child session must not believe it is nested inside the session that queued it."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDECODE") and k != "CLAUDE_CODE_ENTRYPOINT"}
    env.setdefault("TERM", "dumb")
    return env


# ------------------------------------------------------------------------------ the tick

def _spawn(rid: str, resume: bool) -> None:
    if os.environ.get("BUILD_PIPELINE_SYNC") == "1":      # tests, and anyone who wants one run at a time
        manage(rid, resume)
        return
    d = run_dir(rid)
    d.mkdir(parents=True, exist_ok=True)
    with (d / "manager.log").open("a", encoding="utf-8") as fh:
        p = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "manage", rid] + (["--resume"] if resume else []),
                             stdout=fh, stderr=fh, stdin=subprocess.DEVNULL, start_new_session=True)
    write_state(rid, manager_pid=p.pid)


def running() -> list[str]:
    out = []
    for f in Path(RUNS).glob("*/state.json"):
        try:
            if json.loads(f.read_text()).get("status") in LIVE:
                out.append(f.parent.name)
        except (OSError, ValueError):
            continue
    return out


def reap() -> list[str]:
    """Mark `orphaned` only the live-looking runs with a positive dead reading."""
    gone = []
    for rid in running():
        st = read_state(rid)
        verdict, why = run_liveness(st, run_dir(rid))
        if verdict == "dead":
            write_state(rid, status="orphaned", reason=f"reaped: {why}", ended=_iso(_now()))
            log(rid, f"reaped: {why}")
            release_stage_locks(rid)
            gone.append(rid)
    return gone


def tick() -> list[str]:
    """One pass: reap, then launch every due entry whose gates are met, up to the run cap."""
    reap()
    started = []
    for f in Q.due():
        try:
            e = Q.entry(f)
        except (OSError, ValueError):
            continue
        if e["kind"] == "brief" and Q.unmet_requires(e["fm"]):
            Q.defer(f, 30, "waiting on: " + "; ".join(Q.unmet_requires(e["fm"]))[:200])
            continue
        if len(running()) >= C.MAX_RUNS:
            Q.defer(f, 1, "run cap full, waiting for a slot")
            continue
        rid = e["agent"] if e["kind"] == "resume" else f.stem
        if e["kind"] == "resume" and read_state(rid).get("status") not in ("interrupted", "limited"):
            f.rename(Q.DONE / f"{_now():%Y-%m-%d_%H%M}_{f.stem}-superseded.md")
            continue
        f.rename(Q.DONE / f"{_now():%Y-%m-%d_%H%M%S}_{f.stem}.md")
        if e["kind"] == "brief":
            write_state(rid, status="starting", started=_iso(_now()), reason="launching", title=e["title"])
        else:
            write_state(rid, status="starting", reason="resuming")
        _spawn(rid, resume=e["kind"] == "resume")
        started.append(rid)
    return started


def kill(rid: str) -> None:
    st = write_state(rid, status="killed", reason="stopped by hand")
    for pid in (st.get("claude_pid"), st.get("manager_pid")):
        if pid_state(pid) == "alive":
            try:
                os.kill(int(pid), signal.SIGTERM)
            except OSError:
                pass
    log(rid, "killed by hand")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("tick")
    sub.add_parser("status")
    k = sub.add_parser("kill")
    k.add_argument("run_id")
    sub.add_parser("usage")
    m = sub.add_parser("manage")
    m.add_argument("run_id")
    m.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    if args.cmd == "tick":
        for rid in tick():
            print(f"launched {rid}")
        return 0
    if args.cmd == "status":
        for f in sorted(Path(RUNS).glob("*/state.json"), reverse=True):
            st = read_state(f.parent.name)
            print(f"{st.get('status', '?'):12} {f.parent.name}  {st.get('reason', '')[:80]}")
        return 0
    if args.cmd == "kill":
        kill(args.run_id)
        return 0
    if args.cmd == "usage":
        record_usage(sys.stdin.read())
        return 0
    manage(args.run_id, args.resume)
    return 0


if __name__ == "__main__":
    sys.exit(main())

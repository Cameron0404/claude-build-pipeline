#!/usr/bin/env python3
"""bp_runner.py (the default runner) and a whole pipeline end to end, against a fake `claude`.

    python3 tests/runner_selftest.py

What it proves:
  1. pid_state reads a live pid as alive and a finished one as dead; run_liveness needs both
     pids dead and no fresh file before it says dead.
  2. tick launches a due brief, the run records state.json, stream.jsonl and the session id, and a
     clean exit is `done`.
  3. A gated brief is deferred, not launched, while its gate file is missing.
  4. A "Not logged in" answer is `failed` with a reason the supervisor stalls on, and an API error
     is `interrupted` with a resume queued.
  5. The run cap holds: with the cap full, a due brief is deferred.
  6. A small pipeline runs from `queue` to `complete` through tick and supervise alone, every gate
     written by the supervisor, and the worktree lock never left behind.

The fake CLI is tests/fixtures/fake_claude.py. Runs are synchronous (BUILD_PIPELINE_SYNC=1) and
everything lives in a temporary BUILD_PIPELINE_HOME. Stdlib only.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HOME = Path(tempfile.mkdtemp(prefix="bp-runner-selftest-"))
os.environ.update({"BUILD_PIPELINE_HOME": str(HOME), "BUILD_PIPELINE_SYNC": "1", "BUILD_PIPELINE_NO_PUSH": "1",
                   "BUILD_PIPELINE_CLAUDE": str(ROOT / "tests" / "fixtures" / "fake_claude.py"),
                   "BUILD_PIPELINE_MAX_RUNS": "2"})
os.environ.pop("BUILD_PIPELINE_RUNNER", None)
sys.path.insert(0, str(ROOT / "src"))
import bp_queue as Q  # noqa: E402
import bp_runner as R  # noqa: E402
import build_pipeline as B  # noqa: E402

FAILS: list[str] = []


def check(cond, what):
    print(("ok   " if cond else "FAIL ") + what)
    if not cond:
        FAILS.append(what)


def brief(title: str, repo: Path, requires: str = "") -> Path:
    return Q.add_brief(title, f"**Repo: {repo}**\n\nDo the thing.\n", Q._now(), requires=requires)


def main() -> int:
    repo = HOME / "repo"
    repo.mkdir()

    # 1. liveness
    done = subprocess.Popen([sys.executable, "-c", "pass"])
    done.wait()
    check(R.pid_state(os.getpid()) == "alive" and R.pid_state(done.pid) == "dead" and R.pid_state(None) == "unknown",
          "pid_state: alive, dead and unknown")
    rd = HOME / "liveness"
    rd.mkdir()
    check(R.run_liveness({"manager_pid": done.pid, "claude_pid": done.pid}, rd)[0] == "dead",
          "run_liveness: both pids dead and no file written is dead")
    (rd / "progress.md").write_text("x\n")
    check(R.run_liveness({"manager_pid": done.pid, "claude_pid": done.pid}, rd)[0] == "alive",
          "run_liveness: a progress.md written just now keeps the run alive")
    check(R.run_liveness({"manager_pid": done.pid, "claude_pid": os.getpid()}, HOME / "none")[0] == "alive",
          "run_liveness: a live session outlives a dead manager")

    # 2. a clean run
    f = brief("Probe one", repo)
    started = R.tick()
    st = R.read_state(f.stem)
    check(started == [f.stem] and st.get("status") == "done", f"tick launches a due brief and it ends done ({st.get('status')})")
    check(bool(st.get("session_id")) and (R.RUNS / f.stem / "stream.jsonl").exists() and st.get("claude_pid"),
          "the run records its session id, its stream and the session pid")
    check(not (Q.PENDING / f.name).exists() and list(Q.DONE.glob(f"*_{f.stem}.md")),
          "the launched entry moves to done/ as the launch record")

    # 3. a gate holds
    gate = HOME / "gate.ok"
    g = brief("Probe gated", repo, requires=f"file:{gate}")
    R.tick()
    check((Q.PENDING / g.name).exists() and not R.read_state(g.stem), "a brief whose gate file is missing is deferred")
    gate.write_text("ok\n")
    Q.run_now(Q.PENDING / g.name)
    R.tick()
    check(R.read_state(g.stem).get("status") == "done", "once the gate exists and it is due, it runs")

    # 4. failure modes
    os.environ["FAKE_CLAUDE_MODE"] = "auth"
    a = brief("Probe auth", repo)
    R.tick()
    st = R.read_state(a.stem)
    check(st.get("status") == "failed" and B.AUTH_RE.search(st.get("reason", "")),
          "a logged-out session fails with a reason the supervisor stalls on")
    os.environ["FAKE_CLAUDE_MODE"] = "network"
    n = brief("Probe network", repo)
    R.tick()
    st = R.read_state(n.stem)
    check(st.get("status") == "interrupted" and any(e["kind"] == "resume" for e in B.resume_entries(n.stem)),
          "an API error is interrupted, with a resume queued for the same run")
    os.environ["FAKE_CLAUDE_MODE"] = "ok"

    # 5. the run cap
    for name in ("cap-a", "cap-b"):
        R.write_state(name, status="running", manager_pid=os.getpid())
    c = brief("Probe cap", repo)
    R.tick()
    check((Q.PENDING / c.name).exists() and "run cap full" in Q.entry(Q.PENDING / c.name)["reason"],
          "with the run cap full, a due brief is deferred")
    for name in ("cap-a", "cap-b"):
        R.write_state(name, status="done")
    (Q.PENDING / c.name).unlink()

    # 6. end to end: queue a small pipeline and let tick and supervise carry it
    subprocess.run(["git", "-C", str(repo), "init", "-q", "-b", "main"], check=True)
    p = B.queue(B.make_pipeline("Add a greeting command to the tool", str(repo), "small", slug="e2e"), "now")
    for _ in range(40):
        R.tick()
        B.supervise(slug="e2e")
        p = B.load("e2e")
        if p["status"] in ("complete", "stalled"):
            break
    check(p["status"] == "complete", f"a small pipeline runs to complete through tick and supervise ({p['status']})")
    check(all(Path(s["gate"]).exists() for s in p["stages"]), "every stage's gate was written by the supervisor")
    check(not (Path(p["dir"]) / B.LOCK_NAME).exists(), "no worktree lock is left behind")
    check(all(s["retries"] == 0 for s in p["stages"]), "no stage needed a retry")

    print(f"\n{'FAILED ' + str(len(FAILS)) if FAILS else 'all passed'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    try:
        rc = main()
    finally:
        shutil.rmtree(HOME, ignore_errors=True)
    sys.exit(rc)

#!/usr/bin/env python3
"""build_pipeline.py (the /build skill's generator and supervisor), tested in a sandbox.

    python3 tests/selftest.py

What it proves, each from a failure in docs/failure-record.md:
  1. Sizing: a one-line fix is small, an app with a deploy is large, the default is medium, and
     no size has zero research or zero review.
  2. Every brief carries every guard, has `**Repo: <path>**` on line 1 in the form the runner reads,
     and contains no em-dash. Only ship gets push, merge or deploy rights, and only the ones the
     request authorised. The final review never does.
  3. Gates chain: stage 1 is ungated and due now, every later stage waits on the previous stage's
     gate file, entry names are distinct, and `plan` (the dry run) writes nothing.
  4. The supervisor passes a good stage and releases the next; re-queues a done-without-output,
     a waiting last message and a too-fast finish; stalls on a refusal or a login failure
     without retrying; stops after MAX_RETRIES on the same stage; completes the pipeline.
  5. `extend` adds build stages before ship and moves ship's gate, keeping a hand-added gate.
  5b. `resume` continues a stalled pipeline: the stage points at a new entry carrying the old
     entry's gates, the next supervise pass waits on it instead of stalling again, and a retry
     keeps a hand-added `requires:` gate.
  6. Run names carry the day a stage was queued, and released stages wait out a usage wind-down.
  7. Stage checks: a failing or missing test verdict, a missing Human gates or Delivered section,
     an unnamed note, a scheduled wake-up and an overstated duration are each caught, the final
     review never merges, and `gates/merged.ok` appears once the branch is in main.
  8. One run per worktree: a stage whose run reads orphaned while its session is still
     alive is not retried until the session is dead; a retry takes the worktree lock and names its
     attempt; a lock held by a live attempt holds the next retry; a lock whose attempt ended, or
     whose pid is dead, is stale; the run ending releases it; a hand resume refuses while the
     previous attempt may still be working.
  8b. An `interrupted` stage waits on the runner's own resume (`waiting-resume`): no retry when the
     resume runs; a retry under the lock, with the resume entry retired, when the resume has not
     started 30 minutes after it was due; and a retry at once when no resume is queued.

Nothing touches a real queue, runs or builds: BUILD_PIPELINE_HOME is a temporary directory and every
path is redirected into it. No `claude` is called. Stdlib only.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(HERE))
_HOME = tempfile.mkdtemp(prefix="bp-selftest-")
os.environ["BUILD_PIPELINE_HOME"] = _HOME
os.environ["BUILD_PIPELINE_NO_PUSH"] = "1"
for _k in ("BUILD_PIPELINE_RUNNER", "BUILD_PIPELINE_STYLE_GUIDE", "BUILD_PIPELINE_STYLE_CHECK", "BUILD_PIPELINE_OWNER"):
    os.environ.pop(_k, None)
import bp_config as C  # noqa: E402
import bp_queue as Q  # noqa: E402
import build_pipeline as B  # noqa: E402
import bp_runner as runner  # noqa: E402  the default runner

FAILS: list[str] = []


def check(cond, what):
    print(("ok   " if cond else "FAIL ") + what)
    if not cond:
        FAILS.append(what)


def sandbox(tmp: Path):
    for mod, attrs in ((Q, {"QUEUE": tmp / "q", "PENDING": tmp / "q" / "pending", "DONE": tmp / "q" / "done",
                            "RUNS": tmp / "runs"}),
                       (B, {"BUILDS": tmp / "builds", "RUNS": tmp / "runs", "REPORTS": tmp / "runs" / "_reports",
                            "STAMP": tmp / "stamp", "DEV": tmp / "dev"})):
        for k, v in attrs.items():
            setattr(mod, k, v)
    for d in (Q.PENDING, Q.DONE, B.REPORTS):
        d.mkdir(parents=True, exist_ok=True)
    runner.USAGE = tmp / "usage.json"   # a real reading must never hold a sandboxed stage
    runner.BUILD_LOCKS = B.BUILDS        # the runner releases worktree locks from the sandbox only
    runner.RUNS = B.RUNS


def set_usage(pct, resets_in_min=90, age_s=60, week=None):
    """Write a usage reading as the status line would, `age_s` seconds old."""
    now = dt.datetime.now(Q.P)
    data = {"rate_limits": {"five_hour": {"used_percentage": pct,
                                          "resets_at": int((now + dt.timedelta(minutes=resets_in_min)).timestamp())}}}
    if week is not None:
        data["rate_limits"]["seven_day"] = {"used_percentage": week,
                                            "resets_at": int((now + dt.timedelta(days=2)).timestamp())}
    runner.USAGE.write_text(json.dumps({"_written": (now - dt.timedelta(seconds=age_s)).isoformat(), "data": data}))


RULES2 = {"build": "## Verdict\npytest -q, exit 0, 120 pass 0 fail\nsuite: pass\n",
          "plan": "## Human gates\n- none\n",
          "ship": "## Verdict\nexit 0\nsuite: pass\n\n## Delivered\ndelivered: yes, merged into main\n"}


def fake_run(p, s, status="done", minutes=None, output=True, marker=True, report=True, last="Done. Output written.",
             progress="## Done\n", body=None, tools=()):
    """A run directory as the runner leaves it, and the stage's files as a run would write them."""
    entry = s["entry"]
    f = Q.PENDING / f"{entry}.md"
    if f.exists():
        f.rename(Q.DONE / f"{dt.datetime.now():%Y-%m-%d_%H%M}_{entry}.md")
    d = B.RUNS / entry
    d.mkdir(parents=True, exist_ok=True)
    mins = s["expected"] if minutes is None else minutes
    end = B._now()
    start = end - dt.timedelta(minutes=mins)
    (d / "state.json").write_text(json.dumps({"status": status, "started": B._iso(start),
                                              "started_this_launch": B._iso(start), "ended": B._iso(end),
                                              "reason": "finished"}))
    lines = [json.dumps({"type": "assistant", "message": {"content": [{"type": "tool_use", "name": n, "input": {}}]}})
             for n in tools]
    (d / "stream.jsonl").write_text("\n".join(lines + [json.dumps({"type": "result", "result": last})]) + "\n")
    (d / "progress.md").write_text(progress)
    if output:
        text = RULES2.get(s["kind"], "") if body is None else body
        Path(s["output"]).write_text("# out\n\nfindings\n" + text + (f"\n{B.MARKER}\n" if marker else ""))
    if report:
        Path(s["report"]).write_text("report\n")


def stage(p, sid):
    return next(s for s in p["stages"] if s["id"] == sid)


def main() -> int:
    # 1. sizing
    check(B.size_for("fix the typo on the settings button")[0] == "small", "a one-line fix sizes small")
    check(B.size_for("build a full app with a dashboard, deploy it to Vercel, end to end system")[0] == "large",
          "an app with a deploy sizes large")
    check(B.size_for("make the settings page remember my last filter")[0] == "medium", "the default is medium")
    for size in B.SIZES:
        kinds = [s["kind"] for s in B.stage_list(size)]
        check(kinds.count("research") >= 1 and kinds.count("review") >= 1 and kinds.count("final-review") == 1,
              f"{size}: research, review and final review are never zero")
        check(kinds.count("build") >= (2 if size == "small" else 4), f"{size}: enough build stages")
        order = [k for k in kinds if k in ("research", "review")]
        check(order == ["research", "review"] * (len(order) // 2), f"{size}: every research round is followed by a review")
    check(len(B.stage_list("medium")) == 14 and len(B.stage_list("small")) == 8, "stage counts: small 8, medium 14")
    check([s["kind"] for s in B.stage_list("medium")].count("research") == 3, "medium has three research rounds")

    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        sandbox(tmp)
        repo = tmp / "repo"
        repo.mkdir()

        # 2. briefs
        p = B.make_pipeline("Build the recipe app and its landing page", str(repo), "medium", may_merge=True)
        for s in p["stages"]:
            b = B.render_brief(p, s)
            missing = [g for g in B.GUARD_LABELS if g not in b]
            check(not missing, f"brief {s['id']} carries every guard{' (missing ' + ', '.join(missing) + ')' if missing else ''}")
            first = b.splitlines()[0]
            m = runner.BRIEF_REPO_RE.match(first)
            check(bool(m) and m.group(1).strip() == str(repo.resolve()), f"brief {s['id']}: the runner reads the repo from line 1")
            check("\u2014" not in b, f"brief {s['id']} has no em-dash")
            check("**House style.** Write in plain words" in b, f"brief {s['id']}: the House style guard is there")
            check("run_in_background: false" in b, f"brief {s['id']}: the Foreground guard names the Agent flag")
            check("Write the output marker whether the check passes or fails" in b,
                  f"brief {s['id']}: the Stage report guard asks for the style pass on the report")
            check(s["output"] in b and B.MARKER in b and s["report"] in b, f"brief {s['id']} names its output, marker and report")
            owner = s["kind"] == "ship"
            check(("merge --no-ff" in b) == owner, f"brief {s['id']}: merge rights only in ship, never final review")
            check("deploy to the ONE target" not in b, f"brief {s['id']}: no deploy right that was not authorised")
        guide = tmp / "guide.md"
        guide.write_text("# Guide\n<!-- HOUSE-BLOCK start -->\nOpen with the bottom line.\n<!-- HOUSE-BLOCK end -->\n")
        C.STYLE_GUIDE, C.STYLE_CHECK = str(guide), "stylecheck {file}"
        try:
            b = B.render_brief(p, p["stages"][0])
        finally:
            C.STYLE_GUIDE, C.STYLE_CHECK = "", ""
        check("Open with the bottom line" in b, "a configured style guide's HOUSE-BLOCK reaches the House style guard")
        check(f"stylecheck {p['stages'][0]['report']}" in b, "a configured style check runs on the stage report")
        b = B.render_brief(p, p["stages"][0])
        check("**House style.** Write in plain words" in b and "Open with the bottom line" not in b,
              "with no guide configured, the House style guard is one line")
        none = B.make_pipeline("Build X", str(repo), "small", slug="nothing-authorised")
        b = B.render_brief(none, stage(none, "ship"))
        check("kept push, merge and deploy back" in b and "owner-commands.md" in b,
              "with no authority, ship hands merge and deploy to the owner as commands")
        check("run_in_background" in b and "End your turn once every subagent" in b, "the foreground rule is spelled out")

        # 3. queue and gates
        p = B.queue(B.make_pipeline("Build the thing end to end", str(repo), "medium", slug="gate-test"), "now")
        entries = [Q.entry(Q.PENDING / f"{s['entry']}.md") for s in p["stages"]]
        check(len(entries) == len(p["stages"]) == len({e["path"].name for e in entries}), "one distinct queue entry per stage")
        check(entries[0]["requires"] == "" and entries[0]["run_at"] <= B._now() + dt.timedelta(minutes=1),
              "stage 1 is ungated and due now")
        chain = all(entries[i]["requires"] == f"file:{p['stages'][i - 1]['gate']}" for i in range(1, len(entries)))
        check(chain, "every later stage waits on the previous stage's gate file")
        check(all(e["run_at"] > B._now() + dt.timedelta(days=6) for e in entries[1:]), "gated stages are parked, not polled")
        check(all(e["body"].startswith(f"**Repo: {repo.resolve()}**") for e in entries), "the queued body starts with the Repo line")
        check(all(Q.unmet_requires(e["fm"]) for e in entries[1:]), "no gate is met before the supervisor writes it")

        # 4. supervisor
        s1, s2, s3 = p["stages"][:3]
        B.supervise_one(p)
        check(not Path(s1["gate"]).exists(), "a pending stage is left alone")
        fake_run(p, s1)
        B.supervise_one(p)
        check(Path(s1["gate"]).exists(), "a good done stage gets its gate")
        e2 = Q.entry(Q.PENDING / f"{s2['entry']}.md")
        check(e2["run_at"] <= B._now() + dt.timedelta(minutes=1) and not Q.unmet_requires(e2["fm"]),
              "the next stage is released: due now, gate met")
        first2 = s2["entry"]
        fake_run(p, s2, marker=False, minutes=3)
        B.supervise_one(p)
        check(not Path(s2["gate"]).exists() and s2["retries"] == 1 and s2["entry"] != first2,
              "done without the STAGE COMPLETE marker is re-queued, not passed")
        retry = Q.entry(Q.PENDING / f"{s2['entry']}.md")
        check("Supervisor note" in retry["body"] and "Continue from where it stopped" in retry["body"] and not Q.unmet_requires(retry["fm"]),
              "the retry carries the supervisor note and is ready to run")
        fake_run(p, s2, last="Checkpoint saved. Waiting for the other two subagents.", minutes=3)
        B.supervise_one(p)
        check(s2["retries"] == 2, "a last message that says waiting is re-queued")
        check(len(set(s2["entries"])) == 3, "each retry gets its own queue name, even within one minute")
        fake_run(p, s2, minutes=3)
        B.supervise_one(p)
        check(p["status"] == "stalled" and (Path(p["dir"]) / "STALLED.md").exists(),
              "a too-fast finish on the last retry stalls the pipeline (retries counted on the stage)")

        p = B.queue(B.make_pipeline("Build a second thing end to end", str(repo), "small", slug="stall-test"), "now")
        a = p["stages"][0]
        fake_run(p, a, minutes=2)
        (Path(a["output"])).write_text("x\n\n## Depth justification\nthe repo is tiny\n\n" + B.MARKER + "\n")
        B.supervise_one(p)
        check(Path(a["gate"]).exists(), "a fast finish with a depth justification passes")
        b2 = p["stages"][1]
        fake_run(p, b2, progress="status: refused\nThe brief targets option A, the decision file says B.\n")
        n_pending = len(list(Q.PENDING.glob("*.md")))
        B.supervise_one(p)
        check(p["status"] == "stalled" and len(list(Q.PENDING.glob("*.md"))) == n_pending and b2["retries"] == 0,
              "a refusal stalls with no retry (no loop)")
        check("option A" in p["stalled"]["why"], "the stall says why")

        p = B.queue(B.make_pipeline("Build a third thing", str(repo), "small", slug="auth-test"), "now")
        fake_run(p, p["stages"][0], last="Not logged in · Please run /login")
        B.supervise_one(p)
        check(p["status"] == "stalled" and p["stages"][0]["retries"] == 0, "a login failure stalls with no retry")

        p = B.queue(B.make_pipeline("Build a fourth thing", str(repo), "small", slug="fail-test"), "now")
        fake_run(p, p["stages"][0], status="timed-out")
        B.supervise_one(p)
        check(p["stages"][0]["retries"] == 1 and p["status"] == "running", "a timed-out run is retried")
        fake_run(p, p["stages"][0], status="dormant", output=False)
        B.supervise_one(p)
        check(p["stages"][0]["retries"] == 1 and p["status"] == "running", "a dormant run is waited for, not retried")

        p = B.queue(B.make_pipeline("Build a fifth thing", str(repo), "small", slug="full-test"), "now")
        for s in p["stages"]:
            fake_run(p, s)
            B.supervise_one(p)
        check(p["status"] == "complete" and all(Path(s["gate"]).exists() for s in p["stages"]),
              "every stage passing completes the pipeline")

        # 5. extend
        p = B.queue(B.make_pipeline("Build a sixth thing end to end", str(repo), "medium", slug="extend-test"), "now")
        B.extend("extend-test", 6)
        p = B.load("extend-test")
        ids = [s["id"] for s in p["stages"]]
        check(ids[-4:] == ["build-5", "build-6", "ship", "final-review"], "extend inserts build stages before ship")
        ship = stage(p, "ship")
        e = Q.entry(Q.PENDING / f"{ship['entry']}.md")
        check(e["requires"] == f"file:{stage(p, 'build-6')['gate']}", "ship now waits on the new last build stage")
        e5 = Q.entry(Q.PENDING / f"{stage(p, 'build-5')['entry']}.md")
        check(e5["requires"] == f"file:{stage(p, 'build-4')['gate']}", "the new stage waits on the old last build")
        ship_ok = str(tmp / "other-build" / "gates" / "ship.ok")
        e = Q.entry(Q.PENDING / f"{ship['entry']}.md")
        e["fm"]["requires"] = e["fm"]["requires"] + f", file:{ship_ok}"
        Q._write(e["path"], e["fm"], e["body"])
        B.extend("extend-test", 7)
        p = B.load("extend-test")
        e = Q.entry(Q.PENDING / f"{stage(p, 'ship')['entry']}.md")
        check(Q.requires_items(e["fm"]) == [f"file:{stage(p, 'build-7')['gate']}", f"file:{ship_ok}"],
              "extend moves ship's gate and keeps a hand-added gate")

        # 5b. resume, and retries keeping hand-added gates
        p = B.queue(B.make_pipeline("Build a seventh thing", str(repo), "small", slug="resume-test"), "now")
        a, b = p["stages"][:2]
        fake_run(p, a)
        B.supervise_one(p)
        e = Q.entry(Q.PENDING / f"{b['entry']}.md")
        other = str(tmp / "other" / "gates" / "ship.ok")
        e["fm"]["requires"] = e["fm"]["requires"] + f", file:{other}"
        Q._write(e["path"], e["fm"], e["body"])
        Path(other).parent.mkdir(parents=True, exist_ok=True)
        Path(other).write_text("ok\n")
        fake_run(p, b, progress="status: refused\nThe decision file is missing.\n")
        B.supervise_one(p)
        check(p["status"] == "stalled", "resume setup: the refusal stalls the pipeline")
        stalled = (Path(p["dir"]) / "STALLED.md").read_text()
        check(f"build_pipeline.py\" resume resume-test --stage {b['id']}" in stalled and "bp_queue.py add" not in stalled,
              "STALLED.md names the one resume command, not a by-hand re-queue")
        old = b["entry"]
        B.supervise_one(p)
        check(p["status"] == "stalled", "without resume, a stalled pipeline stays stalled")
        n_pending = len(list(Q.PENDING.glob("*.md")))
        B.resume("resume-test", dry=True)
        p = B.load("resume-test")
        check(p["status"] == "stalled" and len(list(Q.PENDING.glob("*.md"))) == n_pending,
              "resume --dry-run queues nothing and changes nothing")
        B.resume("resume-test")
        p = B.load("resume-test")
        b = p["stages"][1]
        check(b["entry"] != old and b["entries"][-1] == b["entry"] and old in b["entries"],
              "resume points the stage at a new entry and keeps the old one in its history")
        check(p["status"] == "running" and b["status"] == "queued" and "stalled" not in p,
              "resume sets the pipeline and the stage running and clears the stall")
        re_ = Q.entry(Q.PENDING / f"{b['entry']}.md")
        check(Q.requires_items(re_["fm"]) == [f"file:{a['gate']}", f"file:{other}"],
              "the resumed entry keeps the stage gate and the hand-added gate")
        check("Resumed by hand" in re_["body"] and old in re_["body"] and re_["body"].startswith(f"**Repo: {repo.resolve()}**"),
              "the resumed brief says it was resumed and names the previous run")
        check(re_["run_at"] <= B._now() + dt.timedelta(minutes=1), "the resumed entry is due now")
        check("resumed b" in (Path(p["dir"]) / "supervisor.log").read_text().replace(b["id"], "b"),
              "resume is logged in supervisor.log")
        B.supervise_one(p)
        check(p["status"] == "running" and not (Q.DONE / "x").exists() and b["status"] != "stalled",
              "the next supervise pass waits on the new entry instead of stalling again")
        fake_run(p, b, marker=False)
        B.supervise_one(p)
        rt = Q.entry(Q.PENDING / f"{b['entry']}.md")
        check(b["retries"] == 1 and Q.requires_items(rt["fm"]) == [f"file:{a['gate']}", f"file:{other}"],
              "a retry keeps the hand-added gate instead of overwriting it with the stage gate alone")
        fake_run(p, b)
        B.supervise_one(p)
        check(Path(b["gate"]).exists(), "the resumed stage passes and the pipeline moves on")
        try:
            B.resume("resume-test", stage_id=b["id"])
            check(False, "resume refuses a stage that already passed")
        except SystemExit:
            check(True, "resume refuses a stage that already passed")

        p = B.queue(B.make_pipeline("Build an eighth thing", str(repo), "small", slug="gaveup-test"), "now")
        g = p["stages"][0]
        f = Q.PENDING / f"{g['entry']}.md"
        f.rename(Q.DONE / f"{dt.datetime.now():%Y-%m-%d_%H%M}_{g['entry']}-gave-up-refused.md")
        B.supervise_one(p)
        check(p["status"] == "stalled", "a start the runner gave up on stalls the pipeline")
        B.resume("gaveup-test")
        p = B.load("gaveup-test")
        B.supervise_one(p)
        check(p["status"] == "running" and (Q.PENDING / f"{p['stages'][0]['entry']}.md").exists(),
              "resume after a gave-up start queues a fresh entry and the supervisor waits on it")

        # 6. run names and usage holds
        # a parked stage is named, and so runs, under the date it was queued, not a week out
        p = B.queue(B.make_pipeline("Build a ninth thing", str(repo), "small", slug="name-test"), "now")
        today = f"{B._now():%Y-%m-%d}"
        check(all(s["entry"].startswith(today) for s in p["stages"]),
              "every stage's entry (the run id) carries today's date, parked ones included")
        check(Q.entry(Q.PENDING / f"{p['stages'][-1]['entry']}.md")["run_at"] > B._now() + dt.timedelta(days=6),
              "the parked stage still runs only when released")

        # the supervisor does not release a stage into the usage wind-down
        a, b = p["stages"][:2]
        set_usage(95, resets_in_min=90)
        fake_run(p, a)
        B.supervise_one(p)
        eb = Q.entry(Q.PENDING / f"{b['entry']}.md")
        check(Path(a["gate"]).exists() and b["status"] == "held" and eb["run_at"] > B._now() + dt.timedelta(minutes=80),
              "wind-down: the passed stage's successor is held to the window reset, not released now")
        B.supervise_one(p)
        log = (Path(p["dir"]) / "supervisor.log").read_text()
        check(log.count(f"held {b['id']}") == 1, "wind-down: the hold is logged once, not every pass")
        set_usage(95, age_s=3600)
        B.supervise_one(p)
        eb = Q.entry(Q.PENDING / f"{b['entry']}.md")
        check(b["status"] == "released" and eb["run_at"] <= B._now() + dt.timedelta(minutes=1),
              "wind-down: a stale reading holds nothing, so the held stage is released")
        set_usage(40, week=97)
        check(B._usage_hold() is not None, "wind-down: the weekly threshold holds too")
        set_usage(40)
        check(B._usage_hold() is None, "wind-down: below the threshold nothing is held")
        set_usage(95)
        fake_run(p, b, marker=False)
        B.supervise_one(p)
        rb = Q.entry(Q.PENDING / f"{b['entry']}.md")
        check(b["retries"] == 1 and rb["run_at"] > B._now() + dt.timedelta(minutes=80),
              "wind-down: a retry is queued for the reset, not into the wind-down")
        runner.USAGE.unlink()

        # a stage deferred for a full run cap must still go before one released after it
        saved_now = Q._now
        Q._now = lambda: dt.datetime.now(Q.P) - dt.timedelta(minutes=10)
        waiter = Q.add_brief("zz waiter", "**Repo: /x**\nbody", Q._now())
        Q._now = saved_now
        Q.defer(waiter, 1, "run cap full, waiting for a slot")
        released = Q.add_brief("zz released later", "**Repo: /x**\nbody", B._now() + dt.timedelta(days=7))
        Q.run_now(released)
        later = B._now() + dt.timedelta(minutes=2)
        order = [x.name for x in Q.due(later) if x.name in (waiter.name, released.name)]
        check(order == [waiter.name, released.name],
              "starvation: a stage deferred for a full cap sorts before one released after it " + str(order))
        waiter.unlink()
        released.unlink()

        # 7. stage checks: test verdicts, authority, notes, human gates, delivery, durations, merges
        def upto(p, sid):
            """Pass every stage before `sid` and return that stage."""
            for s in p["stages"]:
                if s["id"] == sid:
                    return s
                fake_run(p, s)
                B.supervise_one(p)

        def log_of(p):
            return (Path(p["dir"]) / "supervisor.log").read_text()

        # the tests guard names target.md's test command and the suite line
        b = B.render_brief(p, stage(p, "build-1"))
        check("`test_command` in `target.md`" in b and "600 s cap" in b,
              "a brief runs target.md's test_command, split to finish in the foreground")
        check("suite: pass" in b, "the tests guard asks for the `suite: pass|fail` line")

        # the final review never merges; a decision file outranks the Authority paragraph
        mm = B.make_pipeline("Build X", str(repo), "small", slug="final-review-authority", may_merge=True, may_deploy=True)
        fr, sh = B.render_brief(mm, stage(mm, "final-review")), B.render_brief(mm, stage(mm, "ship"))
        check("merge --no-ff" not in fr and "deploy to the ONE" not in fr and "is theirs to run" in fr,
              "the final review gets no merge or deploy right even when the request authorised them")
        check(B.DECISIONS_WIN in sh and B.DECISIONS_WIN in fr, "decision files outrank Authority in ship and final review")

        # the request is not called verbatim, and the plan amends target.md
        check(all("verbatim:" not in B.render_brief(mm, s) for s in mm["stages"]),
              "no brief calls the request verbatim")
        check("## Amendments" in B.render_brief(mm, stage(mm, "plan")) and "as first given" in fr,
              "the plan writes ## Amendments into target.md, and every brief points at them")

        # notes: a note added mid-build reaches every unpassed stage and must be named from the plan on
        p = B.queue(B.make_pipeline("Build a notes thing", str(repo), "small", slug="notes-test"), "now")
        pl = upto(p, "plan")
        saved_now = B._now
        B._now = lambda: dt.datetime.now(Q.P) - dt.timedelta(hours=2)
        try:
            nid = B.add_note("notes-test", "Also make it work offline")
        finally:
            B._now = saved_now
        p = B.load("notes-test")
        pl = stage(p, "plan")
        check(nid == "N1" and "N1" in (Path(p["dir"]) / "notes.md").read_text(), "notes: note N1 is written to notes.md")
        check("Owner's note N1" in Q.entry(Q.PENDING / f"{pl['entry']}.md")["body"] and "Owner's note N1" in Path(pl["brief"]).read_text()
              and "Owner's note N1" not in Path(stage(p, "discover")["brief"]).read_text(),
              "notes: the note is appended to every unpassed stage's brief and pending entry, not to passed ones")
        fake_run(p, pl)
        B.supervise_one(p)
        check(pl["retries"] == 1 and not Path(pl["gate"]).exists(), "notes: a plan that does not name N1 is re-queued")
        fake_run(p, pl, body=RULES2["plan"] + "N1: offline mode handled in build-2.\n")
        B.supervise_one(p)
        check(Path(pl["gate"]).exists(), "notes: a plan that names N1 passes")
        check("Notes from the owner." in B.render_brief(p, pl) and "--text" in B.render_brief(p, pl),
              "notes: the notes guard names notes.md and the note command")

        # Human gates: the plan must list them, and the supervisor sends them when the plan passes
        p = B.queue(B.make_pipeline("Build a gates thing", str(repo), "small", slug="hgates-test"), "now")
        pl = upto(p, "plan")
        fake_run(p, pl, body="no gates section\n")
        B.supervise_one(p)
        check(pl["retries"] == 1, "gates: a plan without ## Human gates is re-queued")
        fake_run(p, pl, body="## Human gates\n- G1: the owner logs in to the hosting provider | needed by: ship\n")
        B.supervise_one(p)
        check(Path(pl["gate"]).exists() and "1 human gate(s); notified the owner" in log_of(p),
              "gates: the plan passes and its human gates are sent to the owner at once")

        # the build Verdict is read; review ready-to-plan is recorded
        b1 = stage(p, "build-1")
        fake_run(p, b1, body="## Verdict\nexit 1\nsuite: fail\n")
        B.supervise_one(p)
        check(b1["retries"] == 1 and "suite: fail" in log_of(p), "a build whose Verdict says suite: fail is re-queued")
        fake_run(p, b1, body="## Verdict\nall good\n")
        B.supervise_one(p)
        check(b1["retries"] == 2 and "no `suite: pass`" in log_of(p), "a Verdict without a suite line is re-queued")
        fake_run(p, b1)
        B.supervise_one(p)
        g = json.loads(Path(b1["gate"]).read_text())
        check(g.get("suite") == "pass" and g.get("exit_codes") == [0], "the gate records the suite line and exit codes")
        p = B.queue(B.make_pipeline("Build a review thing", str(repo), "small", slug="ready-test"), "now")
        rv = upto(p, "review-1")
        fake_run(p, rv, body="1. blocker\n\nready to plan: no, the data source is unknown\n")
        B.supervise_one(p)
        check(json.loads(Path(rv["gate"]).read_text()).get("ready_to_plan") == "no"
              and "ready to plan: no; notified the owner" in log_of(p),
              "the last review's ready to plan: no is recorded and sent")

        # Delivered: ship must say, and a no is sent
        p = B.queue(B.make_pipeline("Build a ship thing", str(repo), "small", slug="deliver-test"), "now")
        sp = upto(p, "ship")
        fake_run(p, sp, body="## Verdict\nexit 0\nsuite: pass\n")
        B.supervise_one(p)
        check(sp["retries"] == 1, "delivered: a ship without ## Delivered is re-queued")
        fake_run(p, sp, body="## Verdict\nexit 0\nsuite: pass\n\n## Delivered\ndelivered: no, the branch waits for the owner's merge\n")
        B.supervise_one(p)
        g = json.loads(Path(sp["gate"]).read_text())
        check(g.get("delivered") == "no" and "delivered: no" in log_of(p) and "owner's merge" in g.get("delivered_note", ""),
              "delivered: ship passes, records delivered: no and sends it to the owner")

        # Scheduled wake-ups: a run that called one is re-queued
        p = B.queue(B.make_pipeline("Build a wake thing", str(repo), "small", slug="wake-test"), "now")
        a = p["stages"][0]
        fake_run(p, a, tools=("ScheduleWakeup",))
        B.supervise_one(p)
        check(a["retries"] == 1 and "ScheduleWakeup" in log_of(p), "wake-up: a run that called ScheduleWakeup is re-queued")
        b = B.render_brief(p, a)
        check("`ScheduleWakeup`, `CronCreate` and `/loop` end it" in b, "wake-up: the Foreground guard forbids scheduled wake-ups")

        # measured minutes in the gate, and an overstated duration flagged
        p = B.queue(B.make_pipeline("Build a timing thing", str(repo), "small", slug="time-test"), "now")
        a = p["stages"][0]
        fake_run(p, a, minutes=20, body="The stage took 1 h 30 min of focused work.\n")
        B.supervise_one(p)
        g = json.loads(Path(a["gate"]).read_text())
        check(g.get("minutes") == 20 and any("says it took 90" in w for w in g.get("warnings", [])),
              "the gate records measured minutes and flags a stated duration 70 minutes over")
        check(B.stated_minutes("Duration: 25 minutes") == 25 and B.stated_minutes("no time here") is None,
              "stated durations are parsed, and absent ones are None")

        # merged.ok: written once the branch is in main, whoever merged it
        gr = tmp / "gitrepo"
        gr.mkdir()
        git = lambda *a: subprocess.run(["git", "-C", str(gr), *a], capture_output=True, text=True)
        git("init", "-q", "-b", "main")
        git("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "base")
        p = B.queue(B.make_pipeline("Build a merge thing", str(gr), "small", slug="merge-test"), "now")
        git("branch", p["branch"])
        git("checkout", "-q", p["branch"])
        git("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "work")
        git("checkout", "-q", "main")
        upto(p, "final-review")
        ok = Path(p["dir"]) / "gates" / "merged.ok"
        check(Path(stage(p, "ship")["gate"]).exists() and not ok.exists(), "merged.ok: not written while the branch is not in main")
        git("merge", "-q", "--no-ff", "-m", "merge", p["branch"])
        B.supervise_one(p)
        check(ok.exists() and "is in main" in log_of(p), "merged.ok: written once the branch is in main")

        # size_for ignores paths and URLs
        check(B.size_for("fix the typo in ~/dev/my-kit/app/site/index.html")[0] == "small",
              "sizing: path words (app, site, deck) do not make a typo fix large")
        check(B.size_for("fix the typo on https://example.com/app/dashboard")[0] == "small",
              "sizing: URL words do not count as scope")

        # a failed release pushes once instead of only logging
        p = B.queue(B.make_pipeline("Build a release thing", str(repo), "small", slug="release-test"), "now")
        saved_run_now = Q.run_now
        Q.run_now = lambda f: (_ for _ in ()).throw(OSError("disk full"))
        try:
            fake_run(p, p["stages"][0])
            B.supervise_one(p)
        finally:
            Q.run_now = saved_run_now
        check(p["stages"][1].get("release_failed") and "could not release" in log_of(p),
              "release: a failed release is logged and marked for a single push")
        B.supervise_one(p)
        check(p["stages"][1]["status"] == "released", "release: the next pass releases it")

        # 8. one run per worktree (retry1 and retry2 of one stage once ran in one worktree at once)
        def run_state(entry, status, age_min=60, **pids):
            f = Q.PENDING / f"{entry}.md"
            if f.exists():
                f.rename(Q.DONE / f"{dt.datetime.now():%Y-%m-%d_%H%M}_{entry}.md")
            d = B.RUNS / entry
            d.mkdir(parents=True, exist_ok=True)
            (d / "state.json").write_text(json.dumps({"status": status, "reason": "zz-test", "started": B._iso(B._now()),
                                                      **pids}))
            for name in ("log.md", "progress.md"):
                (d / name).write_text("x\n")
                t = dt.datetime.now().timestamp() - age_min * 60
                os.utime(d / name, (t, t))

        dead_pr = subprocess.Popen([sys.executable, "-c", "pass"])
        dead_pr.wait()
        dead = dead_pr.pid
        live = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
        try:
            p = B.queue(B.make_pipeline("Build the lock probe", str(repo), "small", slug="lock-test"), "now")
            s1 = p["stages"][0]
            first = s1["entry"]
            # the recorded case: reaped as orphaned, but the Claude session is still working
            run_state(first, "orphaned", manager_pid=dead, claude_pid=live.pid)
            B.supervise_one(p)
            check(s1["entry"] == first and s1["retries"] == 0 and s1["status"] == "waiting-previous",
                  "lock: an orphaned run whose session is alive is not retried")
            check("may still be working" in log_of(p), "lock: the wait is logged")
            n_log = log_of(p).count("may still be working")
            B.supervise_one(p)
            check(s1["retries"] == 0 and log_of(p).count("may still be working") == n_log,
                  "lock: the next pass re-checks and still waits, without logging again")
            # a fresh log.md alone keeps it alive when neither pid can be seen alive
            run_state(first, "orphaned", age_min=2, manager_pid=dead, claude_pid=dead)
            B.supervise_one(p)
            check(s1["retries"] == 0, "lock: a log.md written two minutes ago still counts as alive")
            # now positively dead: the retry goes ahead and takes the lock for its attempt
            run_state(first, "orphaned", manager_pid=dead, claude_pid=dead)
            B.supervise_one(p)
            lock = Path(p["dir"]) / B.LOCK_NAME
            lk = json.loads(lock.read_text()) if lock.exists() else {}
            check(s1["retries"] == 1 and s1["entry"] != first, "lock: once the session is dead the retry is queued")
            check(lk.get("entry") == s1["entry"] and lk.get("stage") == s1["id"] and lk.get("prev") == first,
                  "lock: the retry holds the worktree lock, naming its own attempt")
            check(B.lock_state(p)[0] == "held", "lock: held while the retry is queued")
            # the retry is launched and running; a sandboxed status wrongly marks it orphaned
            retry1 = s1["entry"]
            run_state(retry1, "orphaned", manager_pid=live.pid, claude_pid=live.pid)
            B.supervise_one(p)
            check(s1["retries"] == 1 and s1["entry"] == retry1, "lock: no retry2 while retry1's manager is alive")
            # retry1 ends failed (the manager saw it end): the lock is stale and retry2 may take it
            run_state(retry1, "failed", manager_pid=dead, claude_pid=dead)
            check(B.lock_state(p)[0] == "stale", "lock: a lock whose attempt has ended is stale")
            B.supervise_one(p)
            lk = json.loads(lock.read_text())
            check(s1["retries"] == 2 and lk.get("entry") == s1["entry"] and lk.get("prev") == retry1,
                  "lock: the stale lock is replaced by the next attempt's")
            # the run ending releases it (the runner's manage() and reap() call release_stage_locks)
            check(runner.release_stage_locks(s1["entry"]) == 1 and not lock.exists(),
                  "lock: the runner releases the lock of the run that ended")
            # a lock taken by a supervisor that died before pointing it is stale
            lock.write_text(json.dumps({"stage": s1["id"], "entry": "", "pid": dead}))
            check(B.lock_state(p)[0] == "stale", "lock: a lock whose pid is dead is stale")
            lock.write_text(json.dumps({"stage": s1["id"], "entry": "", "pid": live.pid}))
            check(B.lock_state(p)[0] == "held", "lock: a lock whose pid is alive is held")
            ok, _ = B.take_lock(p, s1, "x")
            check(not ok, "lock: take_lock refuses a held lock")
            lock.unlink()
            # a lock held by a live attempt of another stage holds a retry of this one
            s2 = p["stages"][1]
            other = "zz-test-live-attempt"
            run_state(other, "running", manager_pid=live.pid, claude_pid=live.pid)
            lock.write_text(json.dumps({"stage": s1["id"], "entry": other, "pid": dead}))
            B.supervise_one(p)
            s2_entry, s2_retries = s2["entry"], s2["retries"]
            run_state(s2_entry, "failed", manager_pid=dead, claude_pid=dead)
            B._retry(p, s2, ["zz-test"], "failed", False)
            check(s2["retries"] == s2_retries and s2["entry"] == s2_entry and s2["status"] == "waiting-previous",
                  "lock: a lock held by a live attempt holds the retry")
            # a hand resume refuses while the previous attempt may still be working
            lock.unlink()
            run_state(s2_entry, "orphaned", manager_pid=dead, claude_pid=live.pid)
            p["status"], p["stalled"] = "stalled", {"stage": s2["id"], "why": "zz-test"}
            B._save(p)
            try:
                B.resume(p["slug"], s2["id"])
                refused = False
            except SystemExit as e:
                refused = "may still be working" in str(e)
            check(refused, "lock: resume refuses while the previous attempt's session is alive")
        finally:
            live.kill()
            live.wait()

        # 8b. an interrupted stage waits on the runner's resume instead of starting a second attempt
        def interrupted(slug):
            pp = B.queue(B.make_pipeline("Build the resume probe", str(repo), "small", slug=slug), "now")
            ss = pp["stages"][0]
            run_state(ss["entry"], "interrupted", ended=B._iso(B._now() - dt.timedelta(minutes=45)),
                      manager_pid=dead, claude_pid=dead)
            return pp, ss

        def pending_resumes(entry):
            return [e for e in B.resume_entries(entry) if e["path"].parent == Q.PENDING]

        # (1) the resume runs: the stage waits, then follows the resumed run, and nothing else starts
        p, s1 = interrupted("resume-runs")
        first = s1["entry"]
        Q.add_resume(first, B._now() + dt.timedelta(minutes=5), "api_error, resume in 10 min")
        B.supervise_one(p)
        check(s1["status"] == "waiting-resume" and s1["retries"] == 0 and s1["entry"] == first,
              "resume: an interrupted stage with a resume queued waits as waiting-resume")
        check("waiting on its own resume" in log_of(p), "resume: the wait is logged")
        B.supervise_one(p)
        check(log_of(p).count("waiting on its own resume") == 1, "resume: the wait is logged once")
        run_state(first, "running", age_min=1, manager_pid=dead, claude_pid=dead)
        B.supervise_one(p)
        check(s1["status"] == "running" and s1["retries"] == 0 and s1["entry"] == first
              and not (Path(p["dir"]) / B.LOCK_NAME).exists(),
              "resume: once the resume has started the stage follows it, and no retry is queued")

        # (2) the resume never runs: waits inside the 30 minutes, takes over after them
        p, s1 = interrupted("resume-never-runs")
        first = s1["entry"]
        Q.add_resume(first, B._now() - dt.timedelta(minutes=10), "network")
        B.supervise_one(p)
        check(s1["status"] == "waiting-resume" and s1["retries"] == 0,
              "resume: ten minutes past its due time the stage still waits")
        for e in pending_resumes(first):
            fm = dict(e["fm"])
            fm["run_at"] = (B._now() - dt.timedelta(minutes=31)).strftime("%Y-%m-%dT%H:%M")
            Q._write(e["path"], fm, e["body"])
        B.supervise_one(p)
        lock = Path(p["dir"]) / B.LOCK_NAME
        lk = json.loads(lock.read_text()) if lock.exists() else {}
        check(s1["retries"] == 1 and s1["entry"] != first and lk.get("entry") == s1["entry"],
              "resume: 31 minutes past due with no start, the supervisor retries under the worktree lock")
        check(not pending_resumes(first), "resume: the stale resume entry is retired so it cannot start beside the retry")
        check("the supervisor took over" in log_of(p), "resume: the take-over is logged")

        # (3) no queue entry at all: the supervisor retries at once
        p, s1 = interrupted("resume-no-entry")
        first = s1["entry"]
        B.supervise_one(p)
        check(s1["retries"] == 1 and s1["entry"] != first and s1["status"] != "waiting-resume",
              "resume: with no resume queued for the run, the supervisor retries at once")
        check(json.loads((Path(p["dir"]) / B.LOCK_NAME).read_text()).get("entry") == s1["entry"],
              "resume: that retry holds the worktree lock")

        # the protected-files guard and the follow-up brief format
        check("**Protected files.**" in B.render_brief(p, p["stages"][0]), "protected: every brief carries the guard")
        fr = B.render_brief(p, stage(p, "final-review"))
        check("followups/<slug>.md" in fr and "Judge in isolation" in fr,
              "final review: judge isolation and the follow-up brief format are in the brief")

    # 3b. the dry run writes nothing, through the real CLI
    slug = "selftest-dry-run-probe"
    r = subprocess.run([sys.executable, str(HERE / "build_pipeline.py"), "plan", "--request", "Build a probe app",
                        "--repo", str(HERE.parent), "--slug", slug, "--show-briefs"], capture_output=True, text=True, timeout=60)
    check(r.returncode == 0 and "Dry run" in r.stdout and "**Repo:" in r.stdout, "plan prints the pipeline and briefs")
    check(not (Path(_HOME) / "builds" / slug).exists(), "plan writes nothing")

    print(f"\n{'FAILED ' + str(len(FAILS)) if FAILS else 'all passed'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    try:
        rc = main()
    finally:
        import shutil
        shutil.rmtree(_HOME, ignore_errors=True)
    sys.exit(rc)

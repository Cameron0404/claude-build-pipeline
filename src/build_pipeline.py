#!/usr/bin/env python3
"""build_pipeline.py - one build request in, a staged, gated, supervised pipeline of headless runs out.

    build_pipeline.py plan  --request "TEXT" [--repo PATH] [--size auto|small|medium|large]
                            [--deliverable TEXT] [--deadline TEXT] [--may-merge] [--may-deploy]
                            [--may-push] [--model M] [--slug S] [--show-briefs] [--json]
    build_pipeline.py queue ...the same flags... [--at now|HH:MM|+2h|tonight]
    build_pipeline.py status [SLUG] [--json]      # every pipeline, or one, stage by stage
    build_pipeline.py extend SLUG --build-stages N # the plan stage asks for more build stages
    build_pipeline.py resume SLUG [--stage ID] [--at now] [--dry-run]  # a stalled stage, after fixing its cause
    build_pipeline.py note SLUG --text "..."        # a request added mid-build; later stages must answer it
    build_pipeline.py supervise [--if-due] [--dry-run] [--slug S]
    build_pipeline.py tick                          # the runner's tick, then supervise: call every minute

The `/build` skill (`skill/build/SKILL.md`) calls this. The failures each part answers are in
`docs/failure-record.md`.

`plan` is the dry run: it prints the pipeline and writes nothing. `queue` writes
`<home>/builds/<slug>/` (pipeline.json, briefs/, out/, gates/) and queues EVERY stage at once in the
work queue (`bp_queue.py`): the first due at --at, each later one gated on `file:<builds>/<slug>/gates/<prev>.ok`
and parked a week out, so the runner does not re-check it every 30 minutes.

THE GATE FILES ARE WRITTEN ONLY BY `supervise`. A bare `file:` gate on a stage's output fires the
moment the file exists, before the run ends, and a run recorded `done` while it waited on background
work would pass every gate. `supervise` passes a stage only when its run ended done AND its output
exists, was written after the launch, ends with the `STAGE COMPLETE` marker, the stage report exists,
the last message does not read as waiting, and a fast finish carries a `## Depth justification`. Then
it writes the gate and makes the next stage due now, so a hop takes one tick, not a 30-minute defer. A
stage that fails the check is re-queued with a supervisor note (at most MAX_RETRIES, counted on the
STAGE, not the run id, because every re-queue has a new run id). A refusal or a "Not logged in" is
never retried: the pipeline stalls, writes STALLED.md and notifies once, and `resume` continues it
once the cause is fixed.

Runs are launched and watched by the runner module named in BUILD_PIPELINE_RUNNER (`bp_runner.py`
by default, which calls `claude -p`). Stdlib only. Python 3.9 or later.
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import atomicio  # noqa: E402
import bp_config as C  # noqa: E402
import bp_queue as Q  # noqa: E402

A = importlib.import_module(C.RUNNER)   # the runner interface, see bp_runner.py

HOME = C.HOME
BUILDS = C.BUILDS
RUNS = C.RUNS
REPORTS = C.REPORTS
STAMP = C.STATE / "build-supervise.stamp"
NOTIFICATIONS = C.STATE / "notifications.log"
DEV = C.WORKTREES
DOCS = Path(__file__).resolve().parent.parent / "docs"
FAILURES = DOCS / "failure-record.md"
OWNER = C.OWNER
SUPERVISE_EVERY_S = 120
MAX_RETRIES = 2
PARK_DAYS = 7
MARKER = "STAGE COMPLETE"
FAST_FRACTION = 0.4   # finishing under 40% of the expected minutes needs a depth justification

# Sizing. `rounds` research rounds, each followed by an adversarial review; `builds` build stages.
# `minutes` is the EXPECTED working time per stage kind, the depth floor's yardstick; the run's
# cap is three times it. A small job gets fewer stages, never zero research or review.
SIZES = {
    "small": {"rounds": 1, "builds": 2, "subagents": 2, "sources": 6,
              "minutes": {"discover": 20, "research": 25, "review": 20, "plan": 20, "build": 35,
                          "ship": 25, "final-review": 20}},
    "medium": {"rounds": 3, "builds": 4, "subagents": 3, "sources": 12,
               "minutes": {"discover": 30, "research": 40, "review": 30, "plan": 35, "build": 55,
                           "ship": 35, "final-review": 30}},
    "large": {"rounds": 3, "builds": 6, "subagents": 5, "sources": 20,
              "minutes": {"discover": 40, "research": 55, "review": 40, "plan": 50, "build": 75,
                          "ship": 45, "final-review": 40}},
}
LARGE_WORDS = ("app", "system", "pipeline", "platform", "end to end", "end-to-end", "deploy", "site",
               "product", "dashboard", "integration", "rebuild", "architecture", "engine", "agent",
               "deck", "case", "research", "strategy", "full", "whole", "multi")
SMALL_WORDS = ("fix", "tweak", "rename", "typo", "one line", "small", "flag", "bump", "quick", "minor",
               "button", "copy", "label", "colour", "color", "single")

# "waiting for" only when the run says it of itself: a finished final review was once re-queued
# because its last line asked the owner whether a brief "should keep waiting for the merge".
WAIT_RE = re.compile(r"(?:^|(?<=[.!?:\n])\s*)waiting (?:for|on)|\b((?:i'm|i am|am|still|now) waiting (?:for|on)|wait(?:ing)? until|once (?:it|they|the \w+) (?:finish|report|return)|"
                     r"in the background|still running|will (?:report|commit|continue) (?:once|when)|"
                     r"i'll (?:commit|report|continue) (?:once|when))", re.I)
AUTH_RE = re.compile(r"not logged in|please run /login|invalid api key", re.I)
REFUSED_RE = re.compile(r"^status:\s*refused\b", re.I | re.M)
WAKEUP_TOOLS = ("ScheduleWakeup", "CronCreate")
SUITE_RE = re.compile(r"^\s*[-*]?\s*suite:\s*(pass|fail)\b", re.I | re.M)
DELIVERED_RE = re.compile(r"^\s*[-*]?\s*delivered:\s*(yes|no)\b(.*)$", re.I | re.M)
READY_RE = re.compile(r"ready to plan:\s*\**\s*(yes|no)\b", re.I)
# "took 45 minutes", "Duration: 1h 10", "about 70 min of work": what a stage says of its own time.
STATED_RE = re.compile(r"(?:took|spent|duration|elapsed|ran for|worked for|wall[- ]clock)\D{0,25}?"
                       r"(\d+(?:\.\d+)?)\s*(h|hours?|hrs?|m|mins?|minutes?)\b(?:\s*(\d+)\s*(?:m|mins?|minutes?)\b)?", re.I)
STATED_GAP_MIN = 10
WAIT_STATES = ("starting", "running", "dormant", "limited")
FAIL_STATES = ("failed", "timed-out", "killed", "interrupted", "no-op", "orphaned", "out-of-credits")


def _now() -> dt.datetime:
    return dt.datetime.now(Q.P)


def _iso(t: dt.datetime) -> str:
    return t.isoformat(timespec="seconds")


def _parse_time(s: str):
    if not s:
        return None
    try:
        t = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=Q.P)


# ------------------------------------------------------------------------------ sizing

def size_for(request: str) -> tuple[str, list[str]]:
    """(size, reasons) from the request's words. A default of medium, because the failure record's
    commonest fault was a job sized too small (46 runs under ten minutes), never too large."""
    # Paths and URLs are not scope: "~/dev/my-kit/app/site" scored "app", "site" and "kit".
    t = " " + re.sub(r"\S*(?:/|\\|://)\S*", " ", (request or "").lower()) + " "
    big =[w for w in LARGE_WORDS if re.search(r"\b" + re.escape(w) + r"\b", t)]
    small = [w for w in SMALL_WORDS if re.search(r"\b" + re.escape(w) + r"\b", t)]
    words = len(t.split())
    score = 2 * len(big) - 2 * len(small) + (words > 25) + (words > 60)
    reasons = []
    if big:
        reasons.append("scope words: " + ", ".join(big))
    if small:
        reasons.append("small-job words: " + ", ".join(small))
    reasons.append(f"{words} words in the request")
    if score >= 5:
        return "large", reasons + [f"score {score} >= 5"]
    if score <= -2 or (small and not big and words <= 12):
        return "small", reasons + [f"score {score}"]
    return "medium", reasons + [f"score {score}, the default"]


# ------------------------------------------------------------------------------ the pipeline

def stage_list(size: str, builds: int | None = None) -> list[dict]:
    cfg = SIZES[size]
    m = cfg["minutes"]
    ids = [("discover", "discover", "Discovery: read everything and frame the problem")]
    for k in range(1, cfg["rounds"] + 1):
        ids.append((f"research-{k}", "research", f"Research round {k}"))
        ids.append((f"review-{k}", "review", f"Adversarial review {k}"))
    ids.append(("plan", "plan", "The plan, with the build split into stages and acceptance tests"))
    for k in range(1, (builds or cfg["builds"]) + 1):
        ids.append((f"build-{k}", "build", f"Build stage {k}"))
    ids.append(("ship", "ship", "Compile, full test suite, ship"))
    ids.append(("final-review", "final-review", "Final adversarial review"))
    return [{"id": i, "kind": k, "name": n, "expected": m[k], "max_minutes": 3 * m[k]} for i, k, n in ids]


def _slug(text: str) -> str:
    words = re.sub(r"[^a-z0-9 ]+", " ", (text or "").lower()).split()
    stop = {"build", "a", "an", "the", "me", "for", "to", "of", "and", "i", "want", "please", "that", "with", "my"}
    keep = [w for w in words if w not in stop][:5] or ["build"]
    return "-".join(keep)[:30].strip("-")


def make_pipeline(request: str, repo: str = "", size: str = "auto", deliverable: str = "", deadline: str = "",
                  may_merge: bool = False, may_deploy: bool = False, may_push: bool = False,
                  model: str = "", slug: str = "", now: dt.datetime | None = None) -> dict:
    now = now or _now()
    repo = str(Path(repo or os.getcwd()).expanduser().resolve())
    reasons = [f"size given: {size}"]
    if size in (None, "", "auto"):
        size, reasons = size_for(request)
    base = slug or _slug(request)
    slug = base
    n = 2
    while (BUILDS / slug).exists():
        slug, n = f"{base}-{n}", n + 1
    d = BUILDS / slug
    p = {
        "slug": slug, "request": request.strip(), "repo": repo, "size": size, "sizing": reasons,
        "deliverable": deliverable, "deadline": deadline, "created": _iso(now),
        "authority": {"merge": may_merge, "deploy": may_deploy, "push": may_push},
        "branch": f"build/{slug}", "worktree": str(DEV / f"build-{slug}"),
        "model": model or Q.OPUS, "dir": str(d), "status": "planned",
        "cfg": {k: v for k, v in SIZES[size].items() if k != "minutes"},
        "stages": [],
    }
    for s in stage_list(size):
        _fill_stage(p, s)
        p["stages"].append(s)
    return p


def _fill_stage(p: dict, s: dict) -> None:
    d = Path(p["dir"])
    s.update({"output": str(d / "out" / f"{s['id']}.md"), "gate": str(d / "gates" / f"{s['id']}.ok"),
              "brief": str(d / "briefs" / f"{s['id']}.md"),
              "report": str(REPORTS / f"build-{p['slug']}-{s['id']}-report.md"),
              "title": f"Build {s['id']} {p['slug']}: {s['name']}",   # the id first: queue names cut at 48
              "entry": "", "entries": [], "retries": 0, "status": "planned"})


def _prev(p: dict, sid: str) -> dict | None:
    ids = [s["id"] for s in p["stages"]]
    i = ids.index(sid)
    return p["stages"][i - 1] if i else None


# ------------------------------------------------------------------------------ the briefs

# Every guard carries a bold label. tests/selftest.py checks every label is in every brief, so a
# guard cannot be dropped from one stage kind by an edit to its template.
GUARD_LABELS = ("**Repo:", "**Who queued this.**", "**Foreground only.**", "**Output first.**",
                "**Target from the repo.**", "**Authority.**", "**Tests before any merge.**",
                "**Named branch, own worktree.**", "**Minimum depth.**", "**Checkpoint.**",
                "**Stage report.**", "**This stage only.**", "**Evidence.**", "**House style.**",
                "**Notes from the owner.**", "**Protected files.**")

# Decision files outrank the Authority paragraph: a build's decision said "no merge" and the
# final-review brief still granted one (failure record).
DECISIONS_WIN = ("A decision file or `target.md` that withholds a merge, deploy or push outranks this "
                 "paragraph: if one does, that step belongs to the owner: add it to the owner's commands "
                 "file and name the decision.")


def _commands_file(p: dict) -> str:
    return f"{p['dir']}/owner-commands.md"


def _owner_cmd(p: dict) -> str:
    """How a stage hands a step to the person: one line in owner-commands.md, which `status` prints."""
    return (f"add it to `{_commands_file(p)}` as one line, `- [ ] <title>: <one shell command> (why: ...; "
            "done when: ...)`, and name it in your report")


def _authority(p: dict, s: dict) -> str:
    a = p["authority"]
    owner = s["kind"] == "ship"
    head = ("**Authority.** Stop at a committed branch: pushing, merging into main (a local `git merge` too), "
            "deploying, publishing, sending and creating an external resource (a hosting project, a repo, a domain) "
            "belong to the ship stage. ")
    if s["kind"] == "final-review":
        # The final review judges; shipping stays with ship. Its fixes go on the branch, and a merge of
        # them waits for the owner.
        return head + ("The final review judges and fixes on the branch only. If ship merged and you "
                       f"commit fixes after it, hand the second merge to {OWNER}: {_owner_cmd(p)}, "
                       "so the merge is theirs to run. " + DECISIONS_WIN)
    if not owner:
        return head + ("The ship stage holds them only as far as the request authorised them. A build stage "
                       "once deployed out of turn and left a second live project behind (failure record 10).")
    rights = []
    if a.get("merge"):
        rights.append(f"merge `{p['branch']}` into main with `git -C \"{p['repo']}\" merge --no-ff {p['branch']}` "
                      "(on the branch the checkout already has), only after the full suite is green")
    if a.get("deploy"):
        rights.append("deploy to the ONE target named in `target.md` (that one project name only)")
    if a.get("push"):
        rights.append(f"push `{p['branch']}` to its remote as a fast-forward, and hand any force-push to {OWNER}")
    if not rights:
        return (f"**Authority.** {OWNER[0].upper() + OWNER[1:]} kept push, merge and deploy back in this build, so for "
                f"each one {_owner_cmd(p)}.")
    return (f"**Authority.** The request authorised this build to: " + "; ".join(rights) +
            f". Your authority ends there: for anything not listed, {_owner_cmd(p)}. " + DECISIONS_WIN)


def _tests_guard(p: dict) -> str:
    """The test command a stage runs: the one discovery wrote into target.md. A suite that takes
    longer than the Bash tool's cap must be split, or stages run a subset or background it."""
    return ("**Tests before any merge.** Run the target's FULL test suite, guards and selftests included, exactly "
            "as `test_command` in `target.md` gives it, all of it, before anything is merged, on the base "
            "commit and on the branch, and compare. If one call would pass the Bash tool's 600 s cap, split it "
            "into parts that each finish in the foreground. Record command, exit code, pass and fail counts and the "
            "commit hash in your output's `## Verdict`, with one line `suite: pass` (every failure also present on "
            "the base) or `suite: fail`; the supervisor reads that line. Red holds the merge until the failure is "
            "proven pre-existing on the base commit, with the proof quoted (failure record 6: a merge went in red "
            "and main had to be reset).")


def _house_style() -> str:
    """The House style guard: one line, then the configured guide's HOUSE-BLOCK when there is one,
    read at render so an edit to the guide reaches the next brief."""
    line = "**House style.** " + C.STYLE_LINE
    block = C.style_block()
    if not block:
        return line
    return line + f" Everything you write for a person follows this block from `{C.STYLE_GUIDE}`.\n\n" + block


def _style_check(s: dict) -> str:
    if C.STYLE_CHECK:
        cmd = C.STYLE_CHECK.replace("{file}", s["report"])
        return (f"Before writing the report and your final message, run `{cmd}` and fix what it flags in the "
                "report and the final message only, because `out/*.md` is the record later stages read. ")
    return ("Before writing your final message, read the report through once for the house style and fix it, "
            "leaving `out/*.md` as the record later stages read. ")


def _guards(p: dict, s: dict) -> str:
    d = p["dir"]
    protected = ", ".join(f"`{x}`" for x in C.PROTECTED) or "none configured"
    return "\n\n".join([
        f"**Who queued this.** The `/build` skill queued this brief from {OWNER}'s request, via "
        f"`build_pipeline.py`. Pipeline `{p['slug']}`, stage `{s['id']}` of "
        f"{len(p['stages'])}; the manifest is `{d}/pipeline.json` and every stage brief is in `{d}/briefs/`. "
        "To confirm this brief is genuine, check the manifest.",
        "**Foreground only.** Use subagents freely, and run them, tests and builds in the foreground: pass "
        "`run_in_background: false` on every Agent call (the default is background) and let each Bash command "
        "finish inside its own call. A background command or an `until` loop keeps running after the turn, and "
        "runs were recorded done that way (failure record 1). This run is headless, so `ScheduleWakeup`, "
        "`CronCreate` and `/loop` end it with nothing to wake it, and the supervisor rejects a stage whose run "
        "called either. End your turn once every subagent, test run and build has returned, and leave wake-ups "
        "to the runner.",
        f"**Notes from the owner.** {OWNER[0].upper() + OWNER[1:]} may add requests while the build runs, as `N1`, "
        f"`N2`... in `{d}/notes.md` (`build_pipeline.py note {p['slug']} --text ...`). Read that file before "
        "starting. From the plan stage on, your output must name every note written before your launch by its "
        "id, saying what you did with it or why it belongs to another stage. A note wins over the original "
        "request where they differ. A request once added mid-build was picked up by no stage.",
        f"**Output first.** Write findings into your output file `{s['output']}` as you make them, not at the end, "
        f"so a rotation loses nothing. Its LAST line must be exactly `{MARKER}`, written only when the stage is "
        "finished. Write the output and the stage report BEFORE your final message. The supervisor rejects a "
        "stage without the marker, or whose last message says it is waiting.",
        "**Target from the repo.** " + (
            f"This stage decides the target: write `{d}/target.md` from the repo itself (see below)."
            if s["kind"] == "discover" else
            f"Before any work, read `{d}/target.md` and every decision file it names, and check they still agree "
            "(repo, branch, deliverable, format, front end, deploy target). On a mismatch, write `status: refused` "
            "and the mismatch as the first lines of your checkpoint and output, and stop. Point to decisions in "
            "briefs, and let the decision file be the copy that counts: a shared header once built option A after "
            "B was decided (failure record 3)."),
        _authority(p, s),
        f"**Protected files.** These paths are protected, in a worktree too, where this rule is the only guard: "
        f"{protected}. Change them through a patch file beside your output, and for applying it {_owner_cmd(p)}.",
        _tests_guard(p),
        f"**Named branch, own worktree.** Change the repo only in `{p['worktree']}` on branch `{p['branch']}` "
        f"(`git -C \"{p['repo']}\" worktree add {p['worktree']} -b {p['branch']}` if it is missing, or without `-b` "
        "if the branch exists). Work in your worktree, and leave each shared checkout as you found it: branch, "
        "stash and working files. After every commit run `git symbolic-ref HEAD` and `git rev-parse HEAD`; a "
        "detached HEAD means stop and say so. Two merges once sat on a detached HEAD and were lost from main.",
        f"**Minimum depth.** Expect about {s['expected']} minutes of real work (cap {s['max_minutes']}). The depth "
        f"floor for this stage is below. Finishing in under {int(FAST_FRACTION * s['expected'])} minutes needs a "
        "`## Depth justification` section in the output saying why the floor was met that fast; otherwise keep "
        "going. The supervisor re-queues a fast stage without one.",
        "**Checkpoint.** Keep the run's `progress.md` current (Done, In progress, Next, Files touched, How to "
        "verify) after every meaningful step, and always list the stage's UNMET acceptance items, so a resumed or "
        "rotated session knows what finished means. When a step needs a human answer, note it and continue with the "
        "rest of the work.",
        f"**Stage report.** Write `{s['report']}` before your final message: what you did, what you found, what "
        "you did not do and why, the checks you ran with their results. State a duration only as measured "
        "(`date` at start and end): stages overstated their own time, and the supervisor records the measured "
        f"minutes in the gate and flags a gap over {STATED_GAP_MIN} minutes. Final message: the output path and "
        "five lines. " + _style_check(s) + "Write the output marker whether the check passes or fails.",
        "**This stage only.** Do this stage's work fully and nothing that belongs to a later stage: later stages "
        "are already queued and will run. Better work inside the stage is welcome; running ahead is not.",
        "**Evidence.** Take every figure, source, quote, file, test result and person from what you read or ran. "
        "Cite every external fact (URL and date) and every repo fact (path and line). Say what you could not see.",
        _house_style(),
    ])


def _stage_body(p: dict, s: dict) -> str:
    d, cfg, k = p["dir"], p["cfg"], s["kind"]
    rounds = cfg["rounds"]
    outs = f"`{d}/out/`"
    fails = f"`{FAILURES}`"
    if k == "discover":
        return f"""## This stage: discovery

Read everything the request touches before deciding anything: the repo's CLAUDE.md and docs, the code or notes
the deliverable lives in, recent commits and reports on the same area, any decision files (grep for `decision`,
`D1`, `option`), and the failure record {fails}.

Write `{d}/target.md` with, each as a `key: value` line then a sentence of evidence (path and line):
`repo`, `worktree`, `branch`, `deliverable` (what exists at the end, in which format), `front_end` (framework
and entry point, or none), `deploy_target` (the ONE project or place, or none), `test_command` (the full
suite including guards), `decision_files` (each with the decision quoted), `prerequisites` (branches that must
be merged first, with `git merge-base --is-ancestor` results), `human_gates` (anything only {OWNER} or another
person can do; for each one {_owner_cmd(p)} NOW, not at the end: gates left to ship were met late, if at all),
and `preflight` (every tool, login and paid API the build needs, each checked now, with the result).

If the target cannot be determined from the repo and the request, write `status: refused` with the ONE
question {OWNER} must answer as the first line of the output, and stop there, so the answer settles it.

Then the output: the problem framed in the requester's terms, what already exists, the constraints, the risks,
and at least 10 research questions for round 1, ordered by how much the answer would change the build. Say
whether the `{p['size']}` sizing looks right; if the job is bigger, say so in the first line.

**Depth floor:** every file named in target.md opened and read, all ten questions written, preflight run."""
    if k == "research":
        n = int(s["id"].split("-")[1])
        src = "the research questions in `out/discover.md`" if n == 1 else f"every numbered finding in `out/review-{n - 1}.md`"
        return f"""## This stage: research round {n} of {rounds}

Read all of {outs} first. Answer {src}. Split the questions across at least {cfg['subagents']} foreground
subagents in parallel (one message, several Agent calls, each with `run_in_background: false`), each told to
return sourced findings and the strongest disconfirming evidence it found. Then check their claims yourself:
a subagent's summary is a lead, not a source.

Output: each question or finding by number, the answer, its sources, how confident, and what stays open.
End with `## What changes for the build`.

**Depth floor:** at least {cfg['sources']} distinct sources cited (URLs with dates, or repo paths with lines),
every question or finding answered or marked open with the reason."""
    if k == "review":
        n = int(s["id"].split("-")[1])
        return f"""## This stage: adversarial review {n} of {rounds}

Read all of {outs}. Spawn at least 2 foreground reviewer subagents with distinct lenses, each told to REFUTE,
not to agree: (a) {OWNER}, who will use or submit this, and what would make them reject it; (b) a sceptical
expert in the domain; and (c) the failure record, {fails}: which of those failures this build is walking into.
Verify their objections yourself before keeping them.

Output: numbered findings, each with severity (blocker, major, minor), the evidence, and what would resolve it.
The next stage must answer every one by number, so make each answerable. End with the line `ready to plan: yes` or
`ready to plan: no`, then why. The supervisor records it, and notifies {OWNER} when the last review before the plan says no.

**Depth floor:** at least 8 findings with evidence, at least 2 of them blockers or majors, or a written case that
there are none."""
    if k == "plan":
        nb = sum(1 for x in p["stages"] if x["kind"] == "build")
        return f"""## This stage: the plan

Read all of {outs}. First a table answering every review finding by number (fixed how, or rejected why). Then the
plan: the architecture or structure, every file to create or change, the data, the interfaces, and the risks.

`## Human gates`: one line per thing only {OWNER} or another person can do (a login, a payment decision, a file
only they have, a review), as `- G1: what | needed by: stage`, each also added to `{_commands_file(p)}` NOW, and
what the build does meanwhile. Write `- none` if there are none. The supervisor sends this list the moment the
plan passes, so gates are met during the build, not discovered at ship.

If the plan changes the target (deliverable, format, repo, deploy target) from `target.md` or from the request,
append a `## Amendments` section to `{d}/target.md`: date, what changed, why, and which note or finding caused it.
Later stages read target.md with its amendments as the target, not the first wording of the request.

Split the build into EXACTLY {nb} stages, as `## Stage 1` to `## Stage {nb}`, each with: scope, files, the
acceptance tests (commands that pass or fail, not prose), expected minutes, and what the next stage relies on.
Stage {nb} leaves the work complete and tested on the branch, ready to ship. If the job genuinely needs more
stages, split it, then run `python3 "{Path(__file__).resolve()}" extend {p['slug']} --build-stages N`
and say so. Give each stage its own brief. End with `## Ship`: the full test command, what is merged or
deployed and where (from target.md), and what goes to {OWNER} as a command.

**Depth floor:** every review finding answered, every stage with at least 2 runnable acceptance tests, the
`## Human gates` section written."""
    if k == "build":
        n = int(s["id"].split("-")[1])
        return f"""## This stage: build stage {n}

Read all of {outs}, then build `## Stage {n}` of `out/plan.md` exactly as it is defined, in the worktree, and
commit on the branch with a finding-named message. Run its acceptance tests and every earlier stage's, and fix
until they pass. Run the full test suite at the end.

Output: what was built (files, commits), the acceptance tests with their real output, and `## Verdict`: suite
command, exit code, pass and fail counts, `git symbolic-ref HEAD`, `git rev-parse HEAD`, and the line `suite: pass`
or `suite: fail` (the supervisor re-queues a stage whose Verdict says fail or has no such line). If a human gate from the
plan is unmet, say so in the first line and build what does not depend on it.

**Depth floor:** every acceptance test of stage {n} run and passing, the full suite run, work committed."""
    if k == "ship":
        return f"""## This stage: compile and ship

Read all of {outs}. Confirm every build stage's verdict, then compile the deliverable as target.md defines it
(the build, the deck, the document). Bring main into the branch in the worktree (`git merge main`), then run the
FULL suite, guards included, on that result. Then ship exactly as far as the Authority paragraph allows, in the
order `## Ship` in `out/plan.md` gives. After a merge, prove it: `git -C "{p['repo']}" rev-parse main` and
`git -C "{p['repo']}" merge-base --is-ancestor {p['branch']} main`. After a deploy, fetch the live URL and check
it answers with the new work.

Output: what was compiled, `## Verdict` (suite on the merged result, with its `suite: pass|fail` line), and what
shipped where, with proof, or the lines in `{_commands_file(p)}` that wait for {OWNER}. Then `## Delivered`: the
line `delivered: yes` or `delivered: no`, then exactly what {OWNER} can open, run or use today and where (a path,
a URL, a command), judged against the deliverable in `target.md` and its amendments. A branch that is not merged,
a signpost to a later build, or a partial deliverable is `delivered: no`, with the reason on the same line. Builds
have ended with nothing the requester could use; the supervisor sends a `no` at once.

**Depth floor:** full suite run on the result, every shipped thing proven live or handed over as a command, the
`## Delivered` line written."""
    return f"""## This stage: final adversarial review

Read all of {outs} and use the deliverable itself (run it, open it, click it). Spawn at least 3 foreground judge
subagents: {OWNER} as the user, a sceptical domain expert, and an adversary armed with the failure record {fails}.
Each scores the result out of 10 and lists what fails. Judge in isolation: give each judge the request, the
target with its amendments, the owner's notes and the deliverable itself, and only those, because combining the
judges' scores with the stages' own verdicts is your step after they score, and have them score before they read
any stage output. Fix what can be fixed without new claims, on the branch, with the suite green; hand the merge
over (see Authority). Run `python3 "{Path(__file__).resolve()}" status {p['slug']}` and put its table in the output.

Output: the judges' scores and reasons, what was fixed, a ranked to-do list (each marked as the owner's decision
or build work), the pipeline roll-up (stages, measured durations, retries), and the bottom line in bold at the
top, saying first whether {OWNER} has the deliverable today. Build work too big to fix here becomes a follow-up
brief in `{d}/followups/<slug>.md`: `**Repo: <path>**` on line 1, the finding with its evidence, the fix, the
acceptance tests, and the suite rule. Leave the queueing to {OWNER}: name it in the to-do list so it can be
built with `/build`.

**Depth floor:** three judges, every acceptance test from the plan re-run, the to-do list ranked."""


def render_brief(p: dict, s: dict) -> str:
    head = f"**Repo: {p['repo']}**"
    # Not "verbatim": the request is the first wording, and the plan's amendments and the owner's
    # notes win over it. Stages once built to a stale deliverable line.
    ctx = [f"# Build `{p['slug']}`, stage `{s['id']}`: {s['name']}", "",
           f"**The request, as first given:** {p['request']}",
           f"(Later changes win over it: `## Amendments` in `{p['dir']}/target.md` and the owner's notes in "
           f"`{p['dir']}/notes.md`.)"]
    if p.get("deliverable"):
        ctx.append(f"**Deliverable:** {p['deliverable']}")
    if p.get("deadline"):
        ctx.append(f"**Deadline:** {p['deadline']}")
    ctx.append(f"**Size:** {p['size']} ({'; '.join(p['sizing'])}). Stages in order: "
               + ", ".join(f"`{x['id']}`" for x in p["stages"]) + ".")
    ctx.append(f"**Earlier outputs:** `{p['dir']}/out/` (read every one before starting).")
    return "\n".join([head, "", _guards(p, s), "", "\n".join(ctx), "", _stage_body(p, s), "",
                      f"**Output:** `{s['output']}`, ending with the line `{MARKER}`. **Report:** `{s['report']}`.", ""])


# ------------------------------------------------------------------------------ store

def _save(p: dict) -> None:
    d = Path(p["dir"])
    d.mkdir(parents=True, exist_ok=True)
    atomicio.write_text(d / "pipeline.json", json.dumps(p, indent=2) + "\n")


def load(slug: str) -> dict:
    return json.loads((BUILDS / slug / "pipeline.json").read_text(encoding="utf-8"))


def all_pipelines() -> list[dict]:
    out = []
    for f in sorted(BUILDS.glob("*/pipeline.json")):
        try:
            out.append(json.loads(f.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            continue
    return out


def _entry_file(entry: str) -> Path | None:
    """The queue file of a stage run: still pending, or the newest one the runner moved to done/
    (`<stamp>_<entry>.md`, or with a suffix such as `-gave-up-refused`)."""
    if not entry:
        return None
    f = Q.PENDING / f"{entry}.md"
    if f.exists():
        return f
    done = [x for suffix in ("", "-gave-up-refused", "-already-launched") for x in Q.DONE.glob(f"*_{entry}{suffix}.md")]
    return max(done, key=lambda x: x.name) if done else None


def _requires(p: dict, s: dict, keep_from: str = "") -> str:
    """The stage's gate on the previous stage, plus every other `requires:` item already on the entry
    `keep_from`. A gate added by hand (another pipeline's `file:.../gates/ship.ok`) was once dropped by every
    retry, which wrote `file:<prev gate>` alone. This pipeline's own gates are replaced, never kept,
    so extend can move ship onto the new last build stage."""
    prev = _prev(p, s["id"])
    items = [f"file:{prev['gate']}"] if prev else []
    own = {f"file:{x['gate']}" for x in p["stages"]}
    f = _entry_file(keep_from)
    if f:
        try:
            old = Q.requires_items(Q.entry(f)["fm"])
        except (OSError, ValueError):
            old = []
        items += [x for x in old if x not in own and x not in items]
    return ", ".join(items)


def _queue_stage(p: dict, s: dict, run_at: dt.datetime, body: str, title: str = "", keep_from: str = "") -> str:
    requires = _requires(p, s, keep_from)
    path = Q.add_brief(title or s["title"], body, run_at, model=p["model"], reason="request",
                       max_minutes=s["max_minutes"], requires=requires, name_at=_now())
    s["entry"] = path.stem
    s["entries"].append(path.stem)
    return path.stem


def queue(p: dict, at: str = "now") -> dict:
    d = Path(p["dir"])
    for sub in ("briefs", "out", "gates"):
        (d / sub).mkdir(parents=True, exist_ok=True)
    first = Q.parse_at(at)
    park = first + dt.timedelta(days=PARK_DAYS)
    for i, s in enumerate(p["stages"]):
        body = render_brief(p, s)
        Path(s["brief"]).write_text(body, encoding="utf-8")
        _queue_stage(p, s, first if i == 0 else park, body)
        s["status"] = "queued"
    p["status"] = "running"
    _save(p)
    return p


def extend(slug: str, builds: int) -> dict:
    """Add build stages after the last one (the plan asked for more), and re-gate ship on the new last."""
    p = load(slug)
    have = [s for s in p["stages"] if s["kind"] == "build"]
    if builds <= len(have):
        print(f"build_pipeline: {slug} already has {len(have)} build stages")
        return p
    ship = next(s for s in p["stages"] if s["kind"] == "ship")
    if Path(ship["gate"]).exists():
        raise SystemExit("build_pipeline: ship has already passed; extend refused")
    at = p["stages"].index(ship)
    m = SIZES[p["size"]]["minutes"]["build"]
    new = []
    for k in range(len(have) + 1, builds + 1):
        s = {"id": f"build-{k}", "kind": "build", "name": f"Build stage {k}", "expected": m, "max_minutes": 3 * m}
        _fill_stage(p, s)
        new.append(s)
    p["stages"][at:at] = new
    park = _now() + dt.timedelta(days=PARK_DAYS)
    for s in p["stages"]:           # every brief lists the stages, so all are re-rendered for the record
        Path(s["brief"]).write_text(render_brief(p, s), encoding="utf-8")
    for s in new:
        _queue_stage(p, s, park, render_brief(p, s))
        s["status"] = "queued"
    for s in p["stages"]:           # pending entries get the new stage list; ship waits on the new last build
        f = Q.PENDING / f"{s['entry']}.md"
        if s in new or not s["entry"] or not f.exists():
            continue
        e = Q.entry(f)
        if s is ship:
            e["fm"]["requires"] = _requires(p, s, keep_from=s["entry"])
        Q._write(f, e["fm"], render_brief(p, s))
    _save(p)
    return p


# ------------------------------------------------------------------------------ supervisor

def _run_state(entry: str) -> dict:
    try:
        return json.loads((RUNS / entry / "state.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _last_result(entry: str) -> str:
    f = RUNS / entry / "stream.jsonl"
    last = ""
    try:
        with f.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if '"type": "result"' in line or '"type":"result"' in line:
                    try:
                        last = str(json.loads(line).get("result") or "")
                    except ValueError:
                        continue
    except OSError:
        pass
    return last


def _refused(entry: str) -> str:
    try:
        text = (RUNS / entry / "progress.md").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    m = REFUSED_RE.search(text)
    return text[m.start():m.start() + 300].strip() if m else ""


NOTE_RE = re.compile(r"^- (N\d+) \(([^)]+)\): ", re.M)


def notes(p: dict) -> list[tuple[str, dt.datetime, str]]:
    """The owner's mid-build notes as (id, written, line), oldest first, from `<dir>/notes.md`."""
    try:
        text = (Path(p["dir"]) / "notes.md").read_text(encoding="utf-8")
    except OSError:
        return []
    out = []
    for m in NOTE_RE.finditer(text):
        t = _parse_time(m.group(2))
        if t:
            out.append((m.group(1), t, text[m.start():text.find("\n", m.start())].strip()))
    return out


def add_note(slug: str, text: str) -> str:
    """Record a request the owner made mid-build (one added while a build ran was once picked up by
    no stage). It goes into notes.md, the file every brief tells the stage to read, and
    is appended to the brief of every stage not yet passed and to every pending queue entry, so a
    stage of an older pipeline, whose brief has no notes guard, sees it too."""
    p = load(slug)
    have = notes(p)
    nid = f"N{len(have) + 1}"
    now = _now()
    one = " ".join(text.split())
    line = f"- {nid} ({_iso(now)}): {one}\n"
    f = Path(p["dir"]) / "notes.md"
    if not f.exists():
        f.write_text(f"# Notes from the owner for build {slug}\n\nAdded mid-build with `build_pipeline.py note`. "
                     "A note wins over the original request where they differ.\n\n", encoding="utf-8")
    with f.open("a", encoding="utf-8") as fh:
        fh.write(line)
    add = (f"\n\n## Owner's note {nid} ({now:%d %b %H:%M}), added after this brief was written\n\n{one}\n\n"
           f"It wins over the request where they differ. Name `{nid}` in your output: what you did with it, "
           "or which later stage it belongs to.\n")
    for s in p["stages"]:
        if Path(s["gate"]).exists():
            continue
        b = Path(s["brief"])
        if b.exists():
            b.write_text(b.read_text(encoding="utf-8").rstrip("\n") + add, encoding="utf-8")
        e = Q.PENDING / f"{s['entry']}.md" if s["entry"] else None
        if e is not None and e.exists():
            en = Q.entry(e)
            fm = dict(en["fm"])
            fm.update({"title": en["title"], "kind": en["kind"]})
            Q._write(e, fm, en["body"].rstrip("\n") + add)
    _log(p, f"note {nid} added: {one[:120]}")
    _save(p)
    return nid


def _wakeups(entry: str) -> list[str]:
    """Scheduled wake-up tools the run called. A headless run that schedules one ends, and nothing
    wakes it: one fix branch never ran its suite for this reason."""
    found = []
    try:
        with (RUNS / entry / "stream.jsonl").open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if '"tool_use"' not in line:
                    continue
                for name in WAKEUP_TOOLS:
                    if f'"name": "{name}"' in line or f'"name":"{name}"' in line:
                        found.append(name)
    except OSError:
        pass
    return sorted(set(found))


def _section(text: str, heading: str) -> str | None:
    """The body of `## heading` up to the next `## `, or None when the heading is absent."""
    m = re.search(r"^##\s+" + re.escape(heading) + r"\b.*$", text, re.M | re.I)
    if not m:
        return None
    nxt = re.search(r"^##\s", text[m.end():], re.M)
    return text[m.end():m.end() + nxt.start()] if nxt else text[m.end():]


def _stage_minutes(s: dict, st: dict) -> float | None:
    """Measured working minutes over every run of the stage (retries build on earlier attempts)."""
    started = _parse_time(st.get("started") or st.get("started_this_launch") or "")
    ended = _parse_time(st.get("ended") or "")
    if not (started and ended):
        return None
    mins = (ended - started).total_seconds() / 60
    for other in s.get("entries", []):
        if other == s["entry"]:
            continue
        o = _run_state(other)
        o0, o1 = _parse_time(o.get("started") or ""), _parse_time(o.get("ended") or "")
        if o0 and o1:
            mins += (o1 - o0).total_seconds() / 60
    return mins


def stated_minutes(text: str) -> float | None:
    """The longest duration a stage claims for itself in its output or report, in minutes."""
    best = None
    for m in STATED_RE.finditer(text or ""):
        n, unit, extra = float(m.group(1)), m.group(2).lower(), m.group(3)
        mins = n * 60 + (float(extra) if extra else 0) if unit.startswith("h") else n
        best = mins if best is None else max(best, mins)
    return best


def check_stage(p: dict, s: dict, st: dict) -> list[str]:
    """The problems that stop a `done` stage passing. Empty means the gate may be written."""
    problems = []
    out = Path(s["output"])
    # The first launch, not this one: a run resumed after a rotation or a wind-down has worked since
    # `started`, and timing it from `started_this_launch` made a 68-minute stage read as 4.
    started = _parse_time(st.get("started") or st.get("started_this_launch") or "")
    ended = _parse_time(st.get("ended") or "")
    if not out.exists():
        return [f"the output file {out} does not exist"]
    text = out.read_text(encoding="utf-8", errors="replace")
    if started and dt.datetime.fromtimestamp(out.stat().st_mtime, Q.P) < started - dt.timedelta(minutes=1):
        problems.append("the output file was not written during this run (it predates the launch)")
    tail = [ln.strip() for ln in text.strip().splitlines()[-5:]]
    if MARKER not in tail:
        problems.append(f"the output does not end with `{MARKER}`, so the stage did not finish")
    if not Path(s["report"]).exists():
        problems.append(f"the stage report {s['report']} was not written")
    last = _last_result(s["entry"])
    if last and WAIT_RE.search(last[-600:]):
        problems.append(f"the run's last message says it was waiting: {last[-200:].strip()!r}")
    # A retry builds on the earlier attempts' work, so the floor counts every run of the stage:
    # once a two-hour final review, retried for a false alarm, re-checked it in 4 minutes
    # and was re-queued again as too fast.
    mins = _stage_minutes(s, st) if ended else None
    if mins is not None and mins < FAST_FRACTION * s["expected"] and "## Depth justification" not in text:
        problems.append(f"it finished in {mins:.0f} min against {s['expected']} expected, with no "
                        "`## Depth justification`; the depth floor is probably unmet")
    problems += _section_problems(p, s, st, text)
    return problems


def _section_problems(p: dict, s: dict, st: dict, text: str) -> list[str]:
    """The checks on what a stage called and on the sections its brief asked for: no scheduled
    wake-up, the build and ship Verdict, the plan's Human gates, ship's Delivered line, and the
    owner's notes named by id."""
    problems, k = [], s["kind"]
    woke = _wakeups(s["entry"])
    if woke:
        problems.append(f"the run called {', '.join(woke)}: a headless run that schedules a wake-up ends and "
                        "nothing wakes it, so finish the work in the foreground")
    if k in ("build", "ship"):
        v = _section(text, "Verdict")
        m = SUITE_RE.search(v or "")
        if v is None:
            problems.append("the output has no `## Verdict` section with the suite result")
        elif not m:
            problems.append("the `## Verdict` has no `suite: pass` or `suite: fail` line")
        elif m.group(1).lower() == "fail":
            problems.append("the `## Verdict` says `suite: fail`: fix the failures the base lacks, or prove "
                            "them pre-existing and write `suite: pass` with the proof")
    if k == "plan" and _section(text, "Human gates") is None:
        problems.append("the plan has no `## Human gates` section (write `- none` if there are none)")
    if k == "ship":
        dsec = _section(text, "Delivered")
        if dsec is None or not DELIVERED_RE.search(dsec):
            problems.append("the output has no `## Delivered` section with a `delivered: yes|no` line")
    if k in ("plan", "build", "ship", "final-review"):
        launched = _parse_time(st.get("started") or st.get("started_this_launch") or "")
        first = min((t for t in (_parse_time(_run_state(e).get("started") or "") for e in s.get("entries", []))
                     if t), default=launched)
        missing = [nid for nid, t, _ in notes(p) if first and t < first and not re.search(r"\b" + nid + r"\b", text)]
        if missing:
            problems.append(f"the output does not name the owner's note{'s' if len(missing) > 1 else ''} "
                            f"{', '.join(missing)} (in notes.md): say what was done with each")
    return problems


def _verdict_extras(p: dict, s: dict, st: dict, text: str) -> dict:
    """What the gate records beyond pass: measured minutes, a stated-duration gap, the review's
    ready-to-plan answer, the ship's delivered line and its parsed Verdict exit codes (failure record)."""
    v = {}
    mins = _stage_minutes(s, st)
    if mins is not None:
        v["minutes"] = round(mins)
    try:
        rep = Path(s["report"]).read_text(encoding="utf-8", errors="replace")
    except OSError:
        rep = ""
    said = stated_minutes(text + "\n" + rep)
    if said is not None and mins is not None and said - mins > STATED_GAP_MIN:
        v.setdefault("warnings", []).append(
            f"the stage says it took {said:.0f} min; the runs measured {mins:.0f} (stages have overstated their own durations)")
    if s["kind"] == "review":
        m = READY_RE.findall(text)
        if m:
            v["ready_to_plan"] = m[-1].lower()
    sec = _section(text, "Verdict")
    if sec is not None:
        codes = re.findall(r"\bexit(?: code)?\s*[:=]?\s*(\d+)", sec, re.I)
        if codes:
            v["exit_codes"] = [int(c) for c in codes]
        m = SUITE_RE.search(sec)
        if m:
            v["suite"] = m.group(1).lower()
    if s["kind"] == "ship":
        m = DELIVERED_RE.search(_section(text, "Delivered") or "")
        if m:
            v["delivered"] = m.group(1).lower()
            v["delivered_note"] = m.group(2).strip(" :-")[:300]
    return v


def _after_pass(p: dict, s: dict, verdict: dict, text: str, dry: bool) -> None:
    """Tell the owner, once, what only they can act on: the plan's human gates, a last review that says the
    build is not ready to plan, and a ship that delivered nothing the owner can use."""
    k, slug = s["kind"], p["slug"]
    if k == "plan":
        gates = [ln.strip()[2:] for ln in (_section(text, "Human gates") or "").splitlines()
                 if ln.strip().startswith("- ") and ln.strip()[2:].strip().lower().rstrip(".") != "none"]
        if gates:
            _log(p, f"plan names {len(gates)} human gate(s); notified the owner")
            _push(f"Build {slug}: {len(gates)} thing(s) only you can do", "; ".join(gates)[:600],
                  f"build-gates-{slug}", dry)
    if k == "review" and verdict.get("ready_to_plan") == "no":
        nxt = p["stages"][p["stages"].index(s) + 1] if s is not p["stages"][-1] else None
        if nxt and nxt["kind"] == "plan":
            _log(p, f"{s['id']} says ready to plan: no; notified the owner, the plan stage will answer it")
            _push(f"Build {slug}: the last review says not ready to plan",
                  f"Read {s['output']}. The plan stage runs next and must answer every finding.",
                  f"build-notready-{slug}", dry)
    if k == "ship" and verdict.get("delivered") == "no":
        _log(p, f"ship says delivered: no ({verdict.get('delivered_note', '')}); notified the owner")
        _push(f"Build {slug} shipped, but nothing reached you yet", verdict.get("delivered_note") or s["output"],
              f"build-undelivered-{slug}", dry)


def _merged_gate(p: dict, dry: bool) -> None:
    """Write `gates/merged.ok` once the build branch is in main, whoever merged it (ship, or the
    owner by hand). A follow-up that needs the merge gates on this file, not on ship.ok, which
    passes whether or not anything was merged."""
    ok = Path(p["dir"]) / "gates" / "merged.ok"
    ship = next((x for x in p["stages"] if x["kind"] == "ship"), None)
    if dry or ok.exists() or not ship or not Path(ship["gate"]).exists():
        return
    try:
        r = subprocess.run(["git", "-C", p["repo"], "merge-base", "--is-ancestor", p["branch"], "main"],
                           capture_output=True, timeout=20)
        if r.returncode != 0:
            return
        sha = subprocess.run(["git", "-C", p["repo"], "rev-parse", "main"], capture_output=True, text=True,
                             timeout=20).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return
    ok.write_text(json.dumps({"branch": p["branch"], "main": sha, "seen": _iso(_now())}, indent=2) + "\n",
                  encoding="utf-8")
    _log(p, f"{p['branch']} is in main ({sha[:10]}): wrote gates/merged.ok")


def _log(p: dict, msg: str) -> None:
    line = f"{_now():%Y-%m-%d %H:%M} {msg}"
    with (Path(p["dir"]) / "supervisor.log").open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    print(f"[{p['slug']}] {msg}")


def _push(text: str, note: str, subject: str, dry: bool) -> None:
    """Tell the owner. Always appended to `<home>/state/notifications.log`; when BUILD_PIPELINE_NOTIFY
    is set, that command also runs with the title and the note as its two arguments (a phone push,
    a desktop notification, a chat message: whatever the owner reads)."""
    if dry or os.environ.get("BUILD_PIPELINE_NO_PUSH"):
        return
    try:
        NOTIFICATIONS.parent.mkdir(parents=True, exist_ok=True)
        with NOTIFICATIONS.open("a", encoding="utf-8") as fh:
            fh.write(f"{_now():%Y-%m-%d %H:%M} [{subject}] {text}: {note}\n")
        if C.NOTIFY:
            subprocess.run(shlex.split(C.NOTIFY) + [text, note], capture_output=True, timeout=40)
    except Exception:  # noqa: BLE001  a notification never stops the supervisor
        pass


def _stall(p: dict, s: dict, why: str, dry: bool) -> None:
    s["status"], p["status"] = "stalled", "stalled"
    p["stalled"] = {"stage": s["id"], "why": why, "at": _iso(_now())}
    text = (f"# Build {p['slug']} stalled at `{s['id']}`\n\n**{why}**\n\nRun: `{s['entry']}` "
            f"(`{RUNS / s['entry']}/`). Brief: `{s['brief']}`.\n\nTo continue after fixing the cause, "
            f"run this one command. It re-queues the stage (keeping its gates), points the stage at the new "
            f"run and sets the pipeline running again:\n\n"
            f"    python3 \"{Path(__file__).resolve()}\" resume {p['slug']} --stage {s['id']}\n")
    if not dry:
        (Path(p["dir"]) / "STALLED.md").write_text(text, encoding="utf-8")
    _log(p, f"STALLED at {s['id']}: {why}")
    _push(f"Build {p['slug']} stalled at {s['id']}", why[:300], f"build-stall-{p['slug']}", dry)


# ------------------------------------------------------------------------------ one run per worktree
#
# Failure record 11: a status check run under a sandbox could not read the managers' pids and marked
# live runs orphaned, and this supervisor queued retry1 and then retry2 of the same stage. They ran
# in the same worktree at the same time: three final-review sessions on one worktree, and a build
# stage's retry1 still editing after a rotation while retry2 asked it to stop.
# Now a retry (or a hand resume) first checks that the previous attempt is really gone, and takes
# an exclusive lock for the pipeline's worktree, naming the attempt it queued. Every stage of a
# pipeline shares one worktree, so one lock per pipeline is one run per worktree. The lock sits in
# the pipeline directory, not in the worktree itself, because the build stages commit from the
# worktree and an untracked lock file there would be swept into a commit.

LOCK_NAME = "worktree.lock"
LOCK_UNPOINTED_MIN = 10   # a lock taken but never pointed at a run, older than this, is stale
# Ended states the manager itself wrote after the session exited: nothing can still be working.
# `orphaned` and `killed` are not here: reap() and `kill` write them without the manager seeing the
# session end, so the session may still be editing files.
_LIVE_CHECK = ("starting", "running", "orphaned", "killed")


def attempt_busy(entry: str) -> tuple[bool, str]:
    """(busy, why) for one stage attempt: may it still be working in the worktree?

    Queued and not launched is busy. `starting`, `running`, `orphaned` and `killed` are busy unless
    the runner's run_liveness() gives a positive dead reading: an unknown reading waits, because a
    duplicate session costs more than a supervisor pass."""
    if not entry:
        return False, "no previous attempt"
    if (Q.PENDING / f"{entry}.md").exists():
        return True, f"{entry} is queued and not launched yet"
    st = _run_state(entry)
    if not st:
        return False, f"{entry} has no run state"
    status = st.get("status", "")
    if status in ("dormant", "limited"):
        # Wound down with its own resume queued: the same attempt comes back to this worktree.
        return True, f"{entry} is {status}, its resume is queued"
    if status not in _LIVE_CHECK:
        return False, f"{entry} ended {status}"
    verdict, why = A.run_liveness(st, RUNS / entry)
    if verdict == "dead":
        return False, f"{entry} is {status} and dead ({why})"
    return True, f"{entry} is {status} and {verdict} ({why})"


def _lock_path(p: dict) -> Path:
    return Path(p["dir"]) / LOCK_NAME


def lock_state(p: dict) -> tuple[str, str]:
    """(`free` | `held` | `stale`, why) for the pipeline's worktree lock."""
    f = _lock_path(p)
    try:
        raw = f.read_text(encoding="utf-8")
        age_min = (_now().timestamp() - f.stat().st_mtime) / 60
    except OSError:
        return "free", "no lock"
    try:
        lk = json.loads(raw)
    except ValueError:
        return ("stale", "unreadable lock") if age_min > LOCK_UNPOINTED_MIN else ("held", "lock being written")
    if lk.get("entry"):
        busy, why = attempt_busy(lk["entry"])
        return ("held" if busy else "stale"), why
    ps = A.pid_state(lk.get("pid"))
    if ps == "dead" or (ps != "alive" and age_min > LOCK_UNPOINTED_MIN):
        return "stale", f"taken by pid {lk.get('pid')} ({ps}) and never pointed at a run"
    return "held", f"being taken by pid {lk.get('pid')} ({ps})"


def take_lock(p: dict, s: dict, prev: str) -> tuple[bool, str]:
    """Take the worktree lock for a new attempt of stage `s`. A stale lock is removed first; a held
    one is left and (False, why) returned. O_EXCL makes two supervisors racing for it safe."""
    state, why = lock_state(p)
    if state == "held":
        return False, why
    f = _lock_path(p)
    if state == "stale":
        f.unlink(missing_ok=True)
        _log(p, f"removed a stale worktree lock: {why}")
    f.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(f), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False, "another supervisor took the lock first"
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump({"worktree": p.get("worktree", ""), "stage": s["id"], "prev": prev, "entry": "",
                   "pid": os.getpid(), "taken": _iso(_now())}, fh)
    return True, ""


def point_lock(p: dict, entry: str) -> None:
    """Name the attempt the lock is held for, once it is queued. From then on the lock is held
    while that attempt is queued or alive, and stale once it has ended or is dead."""
    f = _lock_path(p)
    try:
        lk = json.loads(f.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        lk = {}
    lk.update({"entry": entry, "pointed": _iso(_now())})
    atomicio.write_text(f, json.dumps(lk) + "\n")


def release_lock(p: dict, entry: str = "") -> bool:
    """Remove the lock, when it names `entry` (or whatever it names, when `entry` is empty)."""
    f = _lock_path(p)
    try:
        if entry and json.loads(f.read_text(encoding="utf-8")).get("entry") != entry:
            return False
        f.unlink()
        return True
    except (OSError, ValueError):
        return False


def _lock_sweep(p: dict, dry: bool) -> None:
    """Each pass: a lock whose attempt has ended, or is positively dead, is released."""
    if dry or not _lock_path(p).exists():
        return
    state, why = lock_state(p)
    if state == "stale" and release_lock(p):
        _log(p, f"released the worktree lock: {why}")


def _wait_previous(p: dict, s: dict, dry: bool) -> bool:
    """True, and the stage marked waiting, when an earlier attempt may still be working in the
    worktree or holds the lock. Logged once per attempt waited on; the next pass checks again."""
    busy, why = attempt_busy(s["entry"])
    if not busy:
        state, lwhy = lock_state(p)
        busy, why = state == "held", lwhy
    if not busy:
        s.pop("waiting_on", None)
        return False
    if s.get("waiting_on") != why.split(" ")[0]:
        _log(p, f"{s['id']}: not starting another attempt, the previous one may still be working: {why}")
        s["waiting_on"] = why.split(" ")[0]
    s["status"] = "waiting-previous"
    return True


# An `interrupted` run (network loss, an api_error, one model out of credits) has its own resume
# queued by the runner. The supervisor once also queued a retry, so both could start in the
# same worktree. Now it waits on the resume, and takes over with a normal retry only when no resume
# for the run is queued at all, or the resume has not started RESUME_GRACE_MIN after it was due.
RESUME_GRACE_MIN = 30


def resume_entries(agent: str) -> list[dict]:
    """The runner's queued resumes of run `agent`, pending or already consumed, oldest first."""
    out = []
    slug = Q._slug(agent)
    for d in (Q.PENDING, Q.DONE):
        for f in d.glob(f"*resume-{slug}*.md"):
            try:
                e = Q.entry(f)
            except (OSError, ValueError):
                continue
            if e["kind"] == "resume" and e["agent"] == agent:
                out.append(e)
    return sorted(out, key=lambda e: e["run_at"])


def resume_wait(entry: str, st: dict) -> tuple[bool, str]:
    """(wait, why) for a stage whose run ended `interrupted`. Only a resume due after the run
    ended counts: an older one belongs to an earlier interruption that did start."""
    ended = _parse_time(st.get("ended") or "")
    mine = [e for e in resume_entries(entry) if not ended or e["run_at"] >= ended - dt.timedelta(minutes=1)]
    if not mine:
        return False, "no resume of it is queued"
    due = mine[-1]["run_at"]
    deadline = due + dt.timedelta(minutes=RESUME_GRACE_MIN)
    if _now() < deadline:
        return True, f"the runner's resume is due {due:%H:%M}; the supervisor takes over at {deadline:%H:%M} if it has not started"
    return False, f"its resume was due {due:%H:%M} and had not started {RESUME_GRACE_MIN} minutes later"


def _retire_resumes(entry: str) -> None:
    """Move the taken-over run's pending resumes to done/, so they cannot start beside the retry."""
    for e in resume_entries(entry):
        f = e["path"]
        if f.parent == Q.PENDING:
            try:
                f.rename(Q.DONE / f"{_now():%Y-%m-%d_%H%M}_{f.stem}-superseded.md")
            except OSError:
                pass


def _retry(p: dict, s: dict, problems: list[str], status: str, dry: bool) -> None:
    if _wait_previous(p, s, dry):
        return
    if s["retries"] >= MAX_RETRIES:
        _stall(p, s, f"{MAX_RETRIES} retries used and it still ended {status}: " + "; ".join(problems), dry)
        return
    if not dry:
        ok, why = take_lock(p, s, s["entry"])
        if not ok:
            _log(p, f"{s['id']}: retry held, the worktree lock is held: {why}")
            s["status"] = "waiting-previous"
            return
    s["retries"] += 1
    note = (f"\n\n## Supervisor note ({_now():%d %b %H:%M}), retry {s['retries']} of {MAX_RETRIES}\n\n"
            f"The previous attempt, run `{s['entry']}`, ended `{status}` and did not pass the stage check:\n"
            + "".join(f"- {x}\n" for x in problems) +
            f"\nContinue from where it stopped: read its checkpoint `{RUNS / s['entry']}/progress.md` and the output "
            f"`{s['output']}` as it stands, then finish exactly what is listed above. Stay in the foreground to the end.\n")
    body = Path(s["brief"]).read_text(encoding="utf-8") + note
    _log(p, f"{s['id']} ended {status}; re-queued (retry {s['retries']}): " + "; ".join(problems))
    if dry:
        return
    old = s["entry"]
    # The retry number goes before the slug: queue names are cut at 48 characters, and two retries
    # with the same cut name would make the runner retire the second as already launched.
    _queue_stage(p, s, _usage_hold() or _now(), body,
                 title=f"Build {s['id']} retry{s['retries']} {p['slug']}: {s['name']}", keep_from=old)
    point_lock(p, s["entry"])
    s["status"] = "retrying"
    (Path(p["dir"]) / "briefs" / f"{s['id']}.retry{s['retries']}.md").write_text(body, encoding="utf-8")
    _log(p, f"{s['id']}: {old} -> {s['entry']}")


def resume(slug: str, stage_id: str = "", at: str = "now", dry: bool = False) -> dict:
    """Continue a stalled pipeline after the owner fixed the cause. The stalled stage (or --stage) is
    re-queued with its last brief and every gate its entry carried, the new entry becomes the stage's
    run, and the pipeline runs again. Before this, STALLED.md said to re-queue by hand and flip the
    status, and the next supervise pass read the stage's OLD entry, still refused, and stalled again
    (failure record)."""
    p = load(slug)
    sid = stage_id or (p.get("stalled") or {}).get("stage", "")
    if not sid:
        raise SystemExit(f"build_pipeline: {slug} is not stalled; name the stage with --stage")
    s = next((x for x in p["stages"] if x["id"] == sid), None)
    if s is None:
        raise SystemExit(f"build_pipeline: {slug} has no stage {sid}")
    if Path(s["gate"]).exists():
        raise SystemExit(f"build_pipeline: {sid} has already passed; nothing to resume")
    if p.get("status") in ("complete", "cancelled"):
        raise SystemExit(f"build_pipeline: {slug} is {p['status']}; resume refused")
    old = s["entry"]
    f = _entry_file(old)
    if f is not None and f.parent == Q.PENDING:
        raise SystemExit(f"build_pipeline: {sid}'s entry {old} is still pending; the runner will launch it")
    busy, bwhy = attempt_busy(old)
    if not busy:
        state, lwhy = lock_state(p)
        busy, bwhy = state == "held", lwhy
    if busy:
        raise SystemExit(f"build_pipeline: {sid}'s previous attempt may still be working in the worktree "
                         f"({bwhy}); resume refused so two sessions do not share it. Stop it or let it end first.")
    body = Q.entry(f)["body"] if f else Path(s["brief"]).read_text(encoding="utf-8")
    why = (p.get("stalled") or {}).get("why", "")
    n = int(s.get("resumes", 0)) + 1
    body = body.rstrip("\n") + (
        f"\n\n## Resumed by hand ({_now():%d %b %H:%M}), resume {n}\n\n"
        f"The pipeline stalled at this stage" + (f": {why}" if why else "") + f". The owner fixed the cause and "
        f"resumed it. The previous run was `{old or 'none'}`: if it left a checkpoint "
        f"(`{RUNS / old}/progress.md`) or a partial output (`{s['output']}`), read them and build on "
        "them rather than starting over.\n")
    title = f"Build {s['id']} resume{n} {p['slug']}: {s['name']}"
    run_at = Q.parse_at(at)
    if dry:
        print(f"[{slug}] dry run: would queue {title!r} at {run_at:%a %d %b %H:%M} "
              f"requiring {_requires(p, s, old) or 'nothing'}")
        return p
    ok, lwhy = take_lock(p, s, old)
    if not ok:
        raise SystemExit(f"build_pipeline: the worktree lock is held ({lwhy}); resume refused")
    _queue_stage(p, s, run_at, body, title=title, keep_from=old)
    point_lock(p, s["entry"])
    (Path(p["dir"]) / "briefs" / f"{s['id']}.resume{n}.md").write_text(body, encoding="utf-8")
    s["resumes"], s["retries"] = n, 0     # the cause was fixed by hand, so the stage gets its retries back
    s["status"], p["status"] = "queued", "running"
    p.pop("stalled", None)
    _log(p, f"resumed {s['id']} by hand: {old or 'none'} -> {s['entry']}")
    _save(p)
    return p


def _usage_hold() -> dt.datetime | None:
    """When a stage may start, if the usage wind-down is in effect now, else None.

    A stage released near the usage limit launches, reads its context and is stopped within
    minutes, spending a launch and a resume for nothing. So while the runner's reading is at or over
    the 5-hour threshold (or the weekly one), released stages wait for the reset. A reading the
    runner would not act on (stale, absent, below threshold) holds nothing."""
    try:
            u = A.usage_now()
    except Exception:  # noqa: BLE001  no reading is no hold
        return None
    if not u.get("fresh"):
        return None
    now = _now()
    if u.get("week_pct") is not None and u["week_pct"] >= A.WEEKLY_WINDDOWN:
        return (u.get("week_resets_at") or now + dt.timedelta(hours=6)) + dt.timedelta(minutes=5)
    if u.get("pct") is not None and u["pct"] >= A.DEFAULT_THRESHOLD:
        return (u.get("resets_at") or now + dt.timedelta(hours=1)) + dt.timedelta(minutes=2)
    return None


def _hold(p: dict, s: dict, f: Path, until: dt.datetime) -> None:
    """Park a released stage's entry until the usage window resets. Logged once per reset time."""
    e = Q.entry(f)
    fm = dict(e["fm"])
    fm.update({"title": e["title"], "kind": e["kind"], "run_at": until.strftime("%Y-%m-%dT%H:%M"),
               "reason": "held: usage wind-down in effect"})
    fm.setdefault("due_since", _now().strftime("%Y-%m-%dT%H:%M"))
    Q._write(f, fm, e["body"])
    if s.get("held_until") != _iso(until):
        _log(p, f"held {s['id']} until {until:%a %d %b %H:%M}: the usage wind-down is in effect")
    s["status"], s["held_until"] = "held", _iso(until)


def _release(p: dict, s: dict, dry: bool) -> None:
    """Start the next stage now: its gate is met, so the runner launches it on the next minute.
    While the usage wind-down is in effect it is held to the reset instead (`_usage_hold`)."""
    f = Q.PENDING / f"{s['entry']}.md"
    if not s["entry"] or not f.exists() or dry:
        return
    try:
        until = _usage_hold()
        if until:
            _hold(p, s, f, until)
            return
        Q.run_now(f)
        s["status"] = "released"
        s.pop("held_until", None)
        _log(p, f"released {s['id']} ({s['entry']})")
    except Exception as e:  # noqa: BLE001
        # Logged only, a failed release once left a pipeline silently parked a week out.
        # The push goes once per stage; the next pass tries the release again.
        _log(p, f"could not release {s['id']}: {e}")
        if not s.get("release_failed"):
            s["release_failed"] = _iso(_now())
            _push(f"Build {p['slug']}: could not start {s['id']}", f"{e}"[:300], f"build-release-{p['slug']}", dry)


def supervise_one(p: dict, dry: bool = False) -> dict:
    if p.get("status") in ("complete", "stalled"):
        _merged_gate(p, dry)       # the owner may merge by hand after the pipeline finished
    if p.get("status") in ("complete", "stalled", "planned", "cancelled"):
        return p
    _lock_sweep(p, dry)
    stages = p["stages"]
    for i, s in enumerate(stages):
        if Path(s["gate"]).exists():
            s["status"] = "passed"
            continue
        if not s["entry"]:
            break
        if (Q.PENDING / f"{s['entry']}.md").exists():
            if i and Path(stages[i - 1]["gate"]).exists() and s["status"] != "released":
                _release(p, s, dry)
            break
        names = [x.name for x in Q.DONE.glob(f"*_{s['entry']}-gave-up-refused.md")]
        st = _run_state(s["entry"])
        status = st.get("status", "")
        if names and not st:
            _stall(p, s, "the runner gave up starting it (refused starts: concurrency cap or weekly ceiling)", dry)
            break
        if not st or status in WAIT_STATES:
            s["status"] = status or "launching"
            break
        last = _last_result(s["entry"])
        if AUTH_RE.search(last or "") or AUTH_RE.search(st.get("reason") or ""):
            _stall(p, s, "the session was not logged in; log in, then run resume", dry)
            break
        refusal = _refused(s["entry"]) if status in ("done", "refused", "no-op") else ""
        if status == "refused" or refusal:
            _stall(p, s, "the stage refused its brief (never retried automatically): " + (refusal or st.get("reason", "")), dry)
            break
        if status == "done":
            problems = check_stage(p, s, st)
            if not problems:
                verdict = {"stage": s["id"], "run": s["entry"], "passed": _iso(_now()),
                           "started": st.get("started"), "ended": st.get("ended"), "retries": s["retries"]}
                out_text = Path(s["output"]).read_text(encoding="utf-8", errors="replace")
                if "\u2014" in out_text:
                    verdict["warning"] = "the output contains em-dashes"
                verdict.update(_verdict_extras(p, s, st, out_text))
                if not dry:
                    Path(s["gate"]).parent.mkdir(parents=True, exist_ok=True)
                    Path(s["gate"]).write_text(json.dumps(verdict, indent=2) + "\n", encoding="utf-8")
                s["status"] = "passed"
                _log(p, f"{s['id']} passed ({s['entry']})"
                     + (f", {verdict['minutes']} min measured" if "minutes" in verdict else "")
                     + "".join(f"; warning: {w}" for w in verdict.get("warnings", [])))
                _after_pass(p, s, verdict, out_text, dry)
                if i + 1 < len(stages):
                    _release(p, stages[i + 1], dry)
                continue
            _retry(p, s, problems, status, dry)
            break
        if status == "interrupted":
            wait, why = resume_wait(s["entry"], st)
            if wait:
                if s.get("waiting_resume") != why:
                    _log(p, f"{s['id']}: {s['entry']} was interrupted; waiting on its own resume: {why}")
                    s["waiting_resume"] = why
                s["status"] = "waiting-resume"
                break
            s.pop("waiting_resume", None)
            old = s["entry"]
            _retry(p, s, [f"the run ended interrupted ({st.get('reason', '')}) and {why}; the supervisor took over"],
                   status, dry)
            if s["entry"] != old and not dry:
                _retire_resumes(old)
            break
        if status in FAIL_STATES:
            _retry(p, s, [f"the run ended {status}: {st.get('reason', '')}".strip()], status, dry)
            break
        s["status"] = status
        break
    else:
        if p.get("status") != "complete":
            p["status"] = "complete"
            _log(p, "every stage passed: pipeline complete")
            _push(f"Build {p['slug']} complete", f"Final review: {stages[-1]['output']}", f"build-done-{p['slug']}", dry)
    _merged_gate(p, dry)
    if not dry:
        _save(p)
    return p


def supervise(slug: str = "", dry: bool = False, if_due: bool = False) -> int:
    if if_due:
        try:
            if (_now().timestamp() - STAMP.stat().st_mtime) < SUPERVISE_EVERY_S:
                return 0
        except OSError:
            pass
        STAMP.parent.mkdir(parents=True, exist_ok=True)
        STAMP.write_text(_iso(_now()) + "\n")
    for p in all_pipelines():
        if slug and p["slug"] != slug:
            continue
        try:
            supervise_one(p, dry)
        except Exception as e:  # noqa: BLE001 - one broken pipeline must not stop the others
            print(f"build_pipeline: supervising {p.get('slug')} failed: {e}", file=sys.stderr)
    return 0


# ------------------------------------------------------------------------------ printing

def _minutes(st: dict) -> str:
    a, b = _parse_time(st.get("started", "")), _parse_time(st.get("ended", ""))
    return f"{(b - a).total_seconds() / 60:.0f}" if a and b else ""


def table(p: dict, live: bool = False) -> str:
    L = [f"Pipeline `{p['slug']}` ({p['size']}, {len(p['stages'])} stages, status {p['status']}) in {p['repo']}",
         f"Request: {p['request']}", f"Sizing: {'; '.join(p['sizing'])}",
         f"Branch {p['branch']} in {p['worktree']}; authority: "
         + (", ".join(k for k, v in p["authority"].items() if v) or "none (merge, deploy and push go to the owner)"), "",
         "| # | stage | expected min | cap | gate on | status | run min | retries |", "|---|---|---|---|---|---|---|---|"]
    for i, s in enumerate(p["stages"], 1):
        prev = _prev(p, s["id"])
        mins = _minutes(_run_state(s["entry"])) if live and s["entry"] else ""
        L.append(f"| {i} | {s['id']} | {s['expected']} | {s['max_minutes']} | {prev['id'] + '.ok' if prev else 'none'} "
                 f"| {s['status']} | {mins} | {s['retries']} |")
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("plan", "queue"):
        a = sub.add_parser(name)
        a.add_argument("--request", required=True)
        a.add_argument("--repo", default="")
        a.add_argument("--size", default="auto", choices=("auto", "small", "medium", "large"))
        a.add_argument("--deliverable", default="")
        a.add_argument("--deadline", default="")
        a.add_argument("--may-merge", action="store_true")
        a.add_argument("--may-deploy", action="store_true")
        a.add_argument("--may-push", action="store_true")
        a.add_argument("--model", default="")
        a.add_argument("--slug", default="")
        a.add_argument("--show-briefs", action="store_true")
        a.add_argument("--json", action="store_true")
        if name == "queue":
            a.add_argument("--at", default="now")
    s = sub.add_parser("status"); s.add_argument("slug", nargs="?", default=""); s.add_argument("--json", action="store_true")
    e = sub.add_parser("extend"); e.add_argument("slug"); e.add_argument("--build-stages", type=int, required=True)
    r = sub.add_parser("resume"); r.add_argument("slug"); r.add_argument("--stage", default="")
    r.add_argument("--at", default="now"); r.add_argument("--dry-run", action="store_true")
    n = sub.add_parser("note"); n.add_argument("slug"); n.add_argument("--text", required=True)
    v = sub.add_parser("supervise"); v.add_argument("--if-due", action="store_true")
    v.add_argument("--dry-run", action="store_true"); v.add_argument("--slug", default="")
    sub.add_parser("tick", help="the runner's tick, then a supervisor pass: what a scheduler calls every minute")
    args = ap.parse_args()

    if args.cmd == "tick":
        if hasattr(A, "tick"):
            for rid in A.tick() or []:
                print(f"launched {rid}")
        return supervise(if_due=True)

    if args.cmd in ("plan", "queue"):
        if not Path(args.repo or os.getcwd()).expanduser().is_dir():
            print(f"build_pipeline: repo {args.repo} is not a directory", file=sys.stderr)
            return 2
        p = make_pipeline(args.request, args.repo, args.size, args.deliverable, args.deadline, args.may_merge,
                          args.may_deploy, args.may_push, args.model, args.slug)
        if args.cmd == "queue":
            queue(p, args.at)
        if args.json:
            print(json.dumps(p, indent=2))
        else:
            print(table(p))
            if args.cmd == "plan":
                print("\nDry run: nothing written, nothing queued. `queue` with the same flags queues it.")
            else:
                print(f"\nQueued. Manifest {p['dir']}/pipeline.json; the supervisor moves it stage by stage.")
        if args.show_briefs:
            for st in p["stages"]:
                print(f"\n{'=' * 100}\n{render_brief(p, st)}")
        return 0
    if args.cmd == "status":
        ps = [load(args.slug)] if args.slug else all_pipelines()
        print(json.dumps(ps, indent=2) if args.json else "\n\n".join(table(p, live=True) for p in ps) or "no pipelines")
        for p in ps if not args.json else []:
            cmds = Path(p["dir"]) / "owner-commands.md"
            if cmds.exists():
                print(f"\nWaiting for you in {cmds}:\n" + cmds.read_text(encoding="utf-8").strip())
        return 0
    if args.cmd == "extend":
        print(table(extend(args.slug, args.build_stages)))
        return 0
    if args.cmd == "resume":
        print(table(resume(args.slug, args.stage, args.at, args.dry_run)))
        return 0
    if args.cmd == "note":
        nid = add_note(args.slug, args.text)
        print(f"build_pipeline: note {nid} added to {args.slug}; every stage not yet passed will answer it")
        return 0
    return supervise(args.slug, args.dry_run, args.if_due)


if __name__ == "__main__":
    sys.exit(main())

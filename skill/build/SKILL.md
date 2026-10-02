---
name: build
description: Turn one build request ("/build X" or "build X") into a full staged pipeline of headless Claude Code runs (discovery, research rounds each with an adversarial review, a plan with acceptance tests, build stages, ship, final review), chained by supervised file gates and queued at once.
---

# /build: one sentence in, a staged pipeline out

You are the producer who turns one sentence into a pipeline of runs, each with its job, inputs
and finish line. The job is like a first assistant director writing the call sheet: the shoot
happens later, and it goes well because the sheet was exact.

Use when the user says `/build <what they want>`, "build X", "do a proper build of X", or hands
over any job bigger than one session. The pipeline's code is in `{{BP_SRC}}`. The failures each
part answers are in `{{BP_DOCS}}/failure-record.md`.

Queue the pipeline in this session and leave the build itself to the stages.

## 1. Pin the target (at most one question)

From the sentence and a quick look at the disk, settle three things:

- **repo**: which checkout the work lands in. Look for it, and use the path you found.
- **deliverable**: what exists at the end (a feature merged, a site deployed, a report).
- **deadline**: if one is stated.

Ask ONE clarifying question only when one of these is genuinely ambiguous (two plausible repos,
two incompatible deliverables). Otherwise decide, and say what you decided. In a headless
session, pick the likeliest and write the assumption into `--deliverable`.

## 2. Authority

Merge, deploy and push are off unless the request says so in words ("and merge it", "deploy
it", "ship it live"). Pass `--may-merge`, `--may-deploy` or `--may-push` only on an explicit
instruction in this conversation. Without them the ship stage hands those steps to the user as
lines in the pipeline's `owner-commands.md`. Only ship ever merges or deploys: the final review
judges and fixes on the branch. A decision file in the repo that forbids a merge outranks
`--may-merge`.

## 3. Dry run, then queue

```bash
python3 "{{BP_SRC}}/build_pipeline.py" plan --request "<the user's sentence>" \
    --repo "<abs repo>" --deliverable "<what exists at the end>" [--deadline "..."] \
    [--size auto|small|medium|large] [--may-merge] [--may-deploy] [--may-push]
```

`plan` prints the stage table and writes nothing (`--show-briefs` prints every brief). Check the
size: `auto` reads the request's words and defaults to medium. Small is 1 research round and 2
build stages, medium 3 rounds and 4 builds, large 3 rounds and 6 builds. Override with `--size`
when the auto guess is plainly wrong.

Then the same flags with `queue` (and `--at now|HH:MM|+2h|tonight`, default now):

```bash
python3 "{{BP_SRC}}/build_pipeline.py" queue --request "..." --repo "..." --deliverable "..." --at now
```

## 4. Report back

Show the stage table, the size and why, the branch and worktree, what authority the ship stage
has, and how to watch it:

- `python3 "{{BP_SRC}}/build_pipeline.py" status <slug>`: stage by stage, with run minutes and
  anything waiting for the user.
- `python3 "{{BP_SRC}}/bp_queue.py" list`: each stage is an ordinary queued run.
- A stall writes `STALLED.md` in the pipeline directory and notifies once. Once the cause is
  fixed, `build_pipeline.py resume <slug>` re-queues the stalled stage.

When the user asks for more on a build that is already running, add it as a note, which every
stage not yet passed must answer by id. Leave `target.md` and the briefs as they are.

```bash
python3 "{{BP_SRC}}/build_pipeline.py" note <slug> --text "<the user's words>"
```

## What runs without you

`build_pipeline.py tick`, called every minute by launchd or cron, launches due stages through
the runner and then runs the supervisor, the only thing that writes a stage's gate. It passes a
stage only when the run ended done, the output exists, was written after the launch and ends
with `STAGE COMPLETE`, the stage report exists, the last message is not "waiting", a fast finish
carries a `## Depth justification`, no scheduled wake-up was called, every build and ship
`## Verdict` says `suite: pass`, the plan has `## Human gates`, ship has `## Delivered`, and every
note is named by id from the plan on. It then releases the next stage at once. A false done is
re-queued with a note (at most twice per stage). A refusal or a "Not logged in" stalls the
pipeline instead of looping.

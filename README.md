# claude-build-pipeline

Type one build request into Claude Code and get a supervised pipeline of unattended agent runs.

The stages are discovery, research rounds each followed by an adversarial review, a plan with
acceptance tests, build stages, ship and a final review. Each stage opens only when a supervisor
has checked the last one's work.

By Cameron Mills. MIT licensed.

## Why it exists

Long builds done by unattended agents go wrong in ways that repeat. Runs end while a subagent or
a test suite is still running in the background, and get recorded as done. A decision copied
into a brief goes stale and the build targets the wrong thing. Reviews are an optional paragraph,
so most builds get none. A gate keyed on a file opens the moment the file appears, while the run
is still writing it. Retries loop on a brief that was wrong in the first place.

This pipeline was built from a written record of two weeks of those failures (about 260 runs),
which is in [`docs/failure-record.md`](docs/failure-record.md). Every part of the design answers
one of them. The central idea is that "done" is a claim the supervisor checks, not a fact.

## How it works

```
/build "add offline mode to the notes app"
        |
        v
  build_pipeline.py queue  -->  every stage queued at once, each gated on the previous gate file
        |
        v
  tick (every minute)
    |-- runner: launch due stages as headless `claude -p` runs, record state and stream
    `-- supervisor: check each finished stage, write its gate, release the next stage
```

**Stages.** A pipeline runs `discover`, then `research-k` and `review-k` for each round, then
`plan`, `build-1` to `build-n`, `ship` and `final-review`.

| Size | Research rounds (each with an adversarial review) | Build stages | Stages in all |
|---|---|---|---|
| small | 1 | 2 | 8 |
| medium | 3 | 4 | 14 |
| large | 3 | 6 | 16 |

`--size auto` scores the request's words and defaults to medium, because the commonest fault in
the record was a job sized too small. Each stage kind has an expected number of minutes, and its
run is capped at three times that.

**Discovery** writes `target.md` from the repo itself: repo, branch, deliverable, front end,
deploy target, the test command, the decision files it read and a preflight of every tool and
login the build needs. Every later stage re-reads it and refuses on a mismatch.

**Briefs.** Every stage brief is generated from one template and carries the same sixteen
labelled guards, among them the repo on line 1, foreground only, output first with a completion
marker, authority (only ship may merge, deploy or push, and only as the request said), the full
test suite before any merge, one branch in its own git worktree and a minimum depth. A test fails
if any label is missing from any brief.

**The supervisor** is the only writer of gate files. A stage passes only when all of these hold:

1. the run ended `done`
2. the output file was written after the launch and ends with `STAGE COMPLETE`
3. the stage report exists
4. the run's last message does not read as waiting on background work
5. a run shorter than 40% of the expected time carries a `## Depth justification`
6. the run called no scheduled wake-up tool
7. every build and ship `## Verdict` says `suite: pass`
8. the plan has a `## Human gates` section, and ship has a `## Delivered` line
9. every note added mid-build is named by its id, from the plan on

It then writes the gate and makes the next stage due at once. A stage that fails a check is
re-queued with a note listing what was missing, at most twice per stage. A refusal or a
logged-out session stalls the pipeline with one notification instead of looping.

**One run per worktree.** Before a retry, the supervisor checks that the previous attempt has
really gone. A run counts as dead only on a positive reading of both its manager and its session.
Then it takes an exclusive lock for the pipeline's worktree.

**The runner** (`src/bp_runner.py`) launches each due queue entry as `claude -p --output-format
stream-json` in the brief's repo and records `state.json`, `stream.jsonl` and `log.md` for it. It
caps concurrent runs, stops a run at its time cap, queues a resume after a network error or a
usage limit, and releases the worktree lock when a run ends. Another runner can stand in for it
by providing the small interface listed at the top of that file. Steward, the fuller agent manager
this was first built against, is at <https://github.com/Cameron0404/steward>.

### Files

| Path | What it is |
|---|---|
| `src/build_pipeline.py` | The generator, the briefs and the supervisor |
| `src/bp_runner.py` | The default runner around `claude -p` |
| `src/bp_queue.py` | The timed work queue (markdown files with frontmatter) |
| `src/bp_config.py` | Every setting, read from environment variables |
| `skill/build/SKILL.md` | The `/build` skill, installed into `~/.claude/skills/build/` |
| `docs/failure-record.md` | The failures the design answers |
| `bin/build-pipeline` | Wrapper that loads `.env` and runs the CLI |
| `install.sh` | Setup |
| `tests/` | The selftest, the runner test and a fake `claude` CLI |

Per pipeline, in `~/.build-pipeline/builds/<slug>/`: `pipeline.json`, `briefs/`, `out/` (each
stage's output), `gates/`, `target.md`, `notes.md`, `owner-commands.md` (steps handed back to
you), `supervisor.log` and `STALLED.md` when it stops.

## Requirements

- macOS or Linux
- Python 3.9 or later (standard library only, no packages to install)
- git 2.5 or later, for worktrees
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code), logged in, with `claude` on the
  PATH
- A Claude plan with room for long runs. A medium pipeline is fourteen sessions of 20 to 75
  minutes each.

## Setup

1. Clone the repo somewhere it can stay:

   ```bash
   git clone https://github.com/Cameron0404/claude-build-pipeline.git ~/code/claude-build-pipeline
   cd ~/code/claude-build-pipeline
   ```

2. Run the tests, which need neither `claude` nor a network:

   ```bash
   bash tests/run.sh
   ```

   You should see `selftest: all passed` and `runner_selftest: all passed`.

3. Copy the settings file and read it through:

   ```bash
   cp .env.example .env
   ```

   The defaults work as they are. The one to decide on now is permissions (see below).

4. Install the skill and the state directory:

   ```bash
   ./install.sh
   ```

   This checks Python, git and `claude`, creates `~/.build-pipeline/`, and writes the `/build`
   skill to `~/.claude/skills/build/SKILL.md` with this repo's paths filled in.

5. Start the tick. On macOS:

   ```bash
   ./install.sh --launchd
   ```

   This loads a LaunchAgent that runs `bin/build-pipeline tick` every 60 seconds. On Linux run
   `./install.sh --cron` and add the line it prints with `crontab -e`.

6. Try a dry run against any git repo. It prints the stage table and writes nothing:

   ```bash
   bin/build-pipeline plan --request "Add a --version flag to the CLI" --repo ~/code/my-tool
   ```

### Permissions

A headless run cannot answer a permission prompt. A command that is not allowed in advance is
denied, the session carries on without it, and the stage then usually fails its checks (no
passing test verdict) and is retried until the pipeline stalls. You have two choices:

- **Recommended.** Allow the commands builds need in `~/.claude/settings.json` (for example
  `Bash(git:*)`, `Bash(npm test:*)`, `Bash(python3:*)`) and keep the default
  `--permission-mode acceptEdits`. Add deny rules for anything a build must never run, such as
  `Bash(git push:*)` when you want pushes kept for yourself.
- **At your own risk, not recommended.** Set
  `BUILD_PIPELINE_CLAUDE_ARGS="--permission-mode bypassPermissions"` in `.env`. It lets every
  unattended run execute any command without asking. Use it only in a disposable VM, container
  or account.

The briefs keep merges, pushes and deploys in the ship stage and only as authorised, but a brief
is an instruction to a model, not a sandbox. What actually limits a run is outside the pipeline:
Claude Code's permission rules and hooks, and protection on the remote (branch protection, deploy
keys, which credentials the machine holds). Under `bypassPermissions` the permission rules no
longer apply.

## Configuration

Every setting is an environment variable, read from `.env` by `bin/build-pipeline`.

| Variable | Default | What it does |
|---|---|---|
| `BUILD_PIPELINE_HOME` | `~/.build-pipeline` | Pipelines, runs, the queue and stage reports |
| `BUILD_PIPELINE_WORKTREES` | `~/dev` | Where each build's worktree `build-<slug>` is created |
| `BUILD_PIPELINE_TZ` | the machine's zone | IANA zone for queue times such as `03:30` |
| `BUILD_PIPELINE_MODEL` | `opus` | Model for every stage |
| `BUILD_PIPELINE_OWNER` | `the owner` | How briefs name the person who asked |
| `BUILD_PIPELINE_CLAUDE` | `claude` | The Claude Code CLI |
| `BUILD_PIPELINE_CLAUDE_ARGS` | `--permission-mode acceptEdits` | Extra arguments for every run |
| `BUILD_PIPELINE_MAX_RUNS` | `3` | Runs at once |
| `BUILD_PIPELINE_THRESHOLD` | `90` | Five-hour usage percentage at which released stages wait for the reset |
| `BUILD_PIPELINE_WEEKLY` | `95` | The same for the seven-day window |
| `BUILD_PIPELINE_PROTECTED` | `.claude/,CLAUDE.md` | Paths stages must change only through a patch handed to you |
| `BUILD_PIPELINE_STYLE_LINE` | plain words, no em dashes | The house-style sentence in every brief |
| `BUILD_PIPELINE_STYLE_GUIDE` | none | A writing guide whose `HOUSE-BLOCK` goes into every brief |
| `BUILD_PIPELINE_STYLE_CHECK` | none | A command each stage runs on its report, with `{file}` as the path |
| `BUILD_PIPELINE_NOTIFY` | none | A command given a title and a note for each notification |
| `BUILD_PIPELINE_RUNNER` | `bp_runner` | The runner module |

**House style.** By default every brief asks for plain words, no em dashes and lists joined with
commas or "and", and the supervisor notes an em dash in a stage's output as a gate warning (it
never fails a stage for one). Set `BUILD_PIPELINE_STYLE_LINE` to replace that sentence with your
own, and `BUILD_PIPELINE_STYLE_GUIDE` to add a longer guide.

**Notifications.** Stalls, human gates, a "not ready to plan" review, an undelivered ship and a
finished pipeline are always appended to `~/.build-pipeline/state/notifications.log`. Set
`BUILD_PIPELINE_NOTIFY` to send them somewhere you will see them. `.env.example` has a macOS
desktop notification as an example.

**Usage holds.** The supervisor can hold released stages while your usage is near its limit. It
reads `~/.build-pipeline/state/usage.json`, which `src/bp_runner.py usage` writes from the JSON a
Claude Code status line receives. Pipe that JSON into it from your status line script to turn the
holds on. With no reading, nothing is held.

## Usage

In a Claude Code session, the skill does the steps for you:

```
/build add a CSV export to the reports page, and merge it when the tests pass
```

It pins the repo and the deliverable, dry-runs, queues and shows you the stage table. By hand:

```bash
# Dry run: the stage table, and every brief with --show-briefs
bin/build-pipeline plan --request "Add a CSV export to the reports page" \
    --repo ~/code/reports-app --deliverable "export button and endpoint, merged into main" --may-merge

# Queue it for tonight at 03:30, or --at now, 14:00 or +2h
bin/build-pipeline queue --request "Add a CSV export to the reports page" \
    --repo ~/code/reports-app --deliverable "export button and endpoint, merged into main" \
    --may-merge --at tonight

# Watch it, stage by stage, with anything waiting for you
bin/build-pipeline status add-csv-export-reports-page

# Add a request mid-build. Every unpassed stage must answer N1 by id.
bin/build-pipeline note add-csv-export-reports-page --text "Also include the date range in the file name"

# The plan asks for more build stages
bin/build-pipeline extend add-csv-export-reports-page --build-stages 6

# After fixing what stalled it
bin/build-pipeline resume add-csv-export-reports-page

# The runs themselves
python3 src/bp_runner.py status
python3 src/bp_queue.py list
```

A pipeline that finishes has passed every gate. Read `out/final-review.md` first: its bold first
line says whether the deliverable is in your hands today.

## Limitations

- Nothing runs while the machine sleeps. A LaunchAgent catches up on wake, but a laptop with its
  lid shut does no building.
- The waiting-message test is a regular expression over the last 600 characters of a run. It
  catches the phrasings seen so far, not every possible one, so the completion marker remains the
  real guard.
- The verdict checks read fixed lines (`suite:`, `delivered:`). A stage can write `suite: pass`
  falsely. The gate records the parsed exit codes beside it so the final review can see a
  mismatch, but nothing reruns the suite for it.
- The guards are instructions in a prompt. Permissions, hooks and branch protection are what
  actually stop a push or a deploy.
- The default runner is deliberately small. It has no context rotation, no model fallback and no
  cost ledger. Steward has those.
- Pipelines are expensive. A medium build is fourteen long sessions, and a large one sixteen.
- Tested on macOS with Python 3.9 and 3.14. Linux should work through cron, but has not been run
  end to end.

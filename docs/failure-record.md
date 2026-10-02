# The failure record: what went wrong in long agent builds, and the fix for each

This pipeline was designed from a record of two weeks of unattended Claude Code builds on one
personal system: about 260 managed run directories (each run's state, every `result` event in
its stream and every manager log line), about 300 finished queue entries and about 140 run
reports. The briefs, the supervisor and the pipeline's shape each answer one or more of the
failures below. The stage briefs point here, so a reviewer stage can check a build against it.

Over that fortnight 249 runs were recorded done, 3 killed, 2 timed out, 2 refused and 1 did
nothing. The median run took 32 minutes, the 90th percentile 195 and the longest 980, waits
included. Most of what went wrong does not show in those counts. **A `done` from the runner
only means the process exited 0 and the checkpoint changed.** It does not mean the work was
finished, so several failures below are runs that stopped halfway and were still recorded done.

Each fix names where it lives:

- **brief**: a guard printed into every stage brief by `src/build_pipeline.py`.
- **supervisor**: `build_pipeline.py supervise`, run every minute by `tick`.
- **shape**: the shape of the generated pipeline.

## 1. Recorded done while waiting on background work

- **How often:** 7 runs whose last message was a waiting message ("Waiting for the probe's
  first three results", recorded done after one minute; "waiting on the full test suite", with
  its report still holding a placeholder). About 20 more posted a waiting message mid-run.
- **Cost:** the next stage started on a missing input.
- **Cause:** the model backgrounds a command or a subagent, then ends its turn. A prose rule
  against it was already in the brief header and it still happened.
- **Fix:**
  - (brief) Foreground only: background commands and background subagents are out, and the
    turn ends only once everything has returned. Write the output before the final message.
    The output must end with the line `STAGE COMPLETE`.
  - (supervisor) A `done` is provisional. The stage passes only when the output exists, was
    modified after the launch and ends with the marker, and the run's last message does not
    read as waiting. A stage that fails the check is re-queued with a note naming what was
    missing.

## 2. Brief refused after a rotation or resume

- **How often:** 3 runs, 4 refusals. More than half of all briefs named another repo without
  saying so on line 1, so the exposure was wide.
- **Cause:** a rotated session sees only its brief and checkpoint. When the brief contradicts
  the working directory's CLAUDE.md, or reads like "agents can do anything", the session takes
  it for a prompt injection.
- **Fix:**
  - (brief) Line 1 of every stage brief is `**Repo: <path>**`, which the runner turns into the
    working directory.
  - (brief) The next guard says who queued the brief, from which request, and which pipeline
    file holds the plan, so a fresh session can check that the brief is real.
  - (brief) Scope is the stage, with no stretch wording.

## 3. Built for the wrong target

- **How often:** 4 confirmed. A shared header said "build option A" after option B had been
  decided. Two runs started before the branch they depended on was merged. A section was
  written against the wrong option. A slide deck was built as text pages.
- **Cause:** decisions were copied into a shared header and went stale. Nothing re-read the
  decision file before building.
- **Fix:**
  - (shape) Discovery writes `target.md` from the repo itself: repo, branch, deliverable,
    format, front end, deploy target, the decision files it read (with each decision quoted)
    and the prerequisites.
  - (brief) Every later stage re-reads `target.md` and the decision files it names, and stops
    with `status: refused` and a note on any mismatch.
  - (brief) Briefs carry pointers to decisions, never copies of them.

## 4. Too small, or ended too quickly

- **How often:** 46 runs under 10 minutes, about 25 of them legitimate (merges, one-pagers).
  Research fan-outs ran 6 to 19 minutes each, and one pipeline's stages used 10 to 35 of their
  120 minutes.
- **Cause:** the work was sliced by title, not by acceptance criteria, and the model stops at
  the first plausible deliverable.
- **Fix:**
  - (brief) Each stage states its expected minutes and a checkable depth floor (at least N
    sourced URLs, or every acceptance test run).
  - (brief) A stage that ends under 40% of its expected time adds `## Depth justification`, or
    keeps working.
  - (supervisor) A fast stage with no justification is re-queued.

## 5. No research, or no adversarial pass

- **How often:** only 59 of 258 briefs named a review or critic pass, and 74 mentioned research,
  so about three builds in four had no review. One stage lost the full text of a review in a
  rotation.
- **Cause:** reviews were optional prose, and their findings lived in session context, not in
  a file.
- **Fix:**
  - (shape) Research and adversarial review are stages, not paragraphs: at least one of each
    for a small job and three of each by default.
  - (shape) Each review writes its findings to a named file, and the next stage is gated on it.
  - (brief) Reviewers are told to refute, and the stage that follows must answer every
    numbered finding.

## 6. Merged with a failing guard

- **How often:** 1 clear case (a merge that failed 2 of 560 tests, after which main was
  reset), plus about 3 merged over "pre-existing" failures.
- **Cause:** the merge was not gated on a recorded test verdict, and the builder had ended
  before the suite reported (failure 1).
- **Fix:**
  - (brief) Every build stage writes a verdict: suite command, exit code, pass and fail counts,
    commit hash, branch, and the line `suite: pass` or `suite: fail`.
  - (brief) Ship re-runs the full suite on the merge result and refuses to merge on red. The
    only exception is a failure proven pre-existing on the base commit, with the proof quoted.
  - (supervisor) A build or ship verdict that says `suite: fail`, or has no suite line, is
    re-queued.

## 7. Detached HEAD, and the shared checkout switched

- **How often:** 1 loss and 1 switching incident. Two merges sat on a detached HEAD while
  `main` stayed put, and the report claimed main had moved. Three runs ran `git checkout -b` in
  the shared checkout at the same time, and one moved everybody's commits onto its branch.
- **Fix:**
  - (brief) Work only in `git worktree add <dir> -b <branch>`, and leave each shared checkout as
    you found it.
  - (brief) After every commit, record `git symbolic-ref HEAD` and `git rev-parse HEAD`. A
    stage that finds itself detached stops and says so.
  - (brief) Merges are `git -C <checkout> merge --no-ff <branch>`, then `git rev-parse main` to
    prove it.

## 8. Queue retry loops on refusals

- **How often:** 34 re-queues after refused starts, 4 entries that gave up, and one stale brief
  re-queued three times running.
- **Cause:** retries were counted on the run id, and every re-queue has a new one, so the cap
  never bit. The wrong brief stayed wrong.
- **Fix:**
  - (supervisor) Retries are counted on the pipeline stage, at most 2 a stage.
  - (supervisor) A content refusal (`status: refused`) is never retried. The pipeline stalls,
    writes `STALLED.md` and notifies once.

## 9. Usage-limit pauses and resumes mid-build

- **How often:** 36 dormant events over 29 runs. One run went dormant 4 times and never
  finished. One resume ended "done" after one minute. In one evening, 11 runs got "Not logged
  in" answers, which read as success events.
- **Fix:**
  - (brief) The checkpoint always carries the stage's unmet acceptance items, so a resumed or
    rotated session knows what "finished" means.
  - (supervisor) It waits while a run is dormant or limited. "Not logged in" is an auth
    failure: the pipeline stalls and notifies once, with no retry loop.
  - (supervisor) While the usage reading is over the threshold, a released stage is held to
    the reset instead of launching into the wind-down.

## 10. Smaller failures

- **Report missing:** 6 briefs named a report that was never written.
  - (brief) Write the stage report before the final message. (supervisor) Check it exists.
- **Push attempts:** 2, both denied by a hook.
  - (brief) Push, merge and deploy rights appear only in ship, and only as the request
    authorised them.
- **A stage doing a later stage's work:** a plan defined six build stages against four
  generated briefs. One stage absorbed three of them and deployed to production before the
  stage authorised to deploy, which then deployed to a second project and left the first live.
  - (shape) The plan must fit the generated build count, or run `build_pipeline.py extend`.
  - (brief) Do only this stage.
- **Gates fired early, or waited 30 minutes:** a `file:` gate fires the moment the file
  exists, so a stage started while the previous one was still running. Otherwise every hop
  waited out a 30-minute defer.
  - (supervisor) Gates are `gates/<stage>.ok` files that only the supervisor writes, after the
    run ended and its output was checked. It then makes the next stage due at once.
- **Decisions lost to rotation:** 126 context rotations over 63 runs.
  - (brief) Findings go into the stage output as they are made.
- **Environment discovered mid-stage:** missing Python packages and a paid API that needed a
  card.
  - (shape) Discovery runs a preflight (tools, logins, paid APIs) into `target.md`.
- **Human dependencies ignored:** a plan said "once the customer data has arrived", and the chain
  built it anyway.
  - (brief) The plan names `## Human gates`, which the supervisor sends the moment the plan
    passes.

## 11. Live runs marked orphaned, and two attempts of one stage in one worktree

Found after the pipeline had run for a day.

- **How often:** 9 live runs marked orphaned in a quarter of an hour, and at least 3 stages then
  had two attempts at once (three final-review sessions on one worktree, a retry still editing
  after a rotation while the next retry asked it to stop).
- **Cause:** a status check ran under a sandbox, where `ps` fails and `os.kill(pid, 0)` on
  another process raises `PermissionError`, which was read as dead. The run was marked orphaned,
  which the supervisor counts as a failure, so it queued a retry into the worktree the live run
  was still using. A Claude session can also outlive its manager, and only the manager's pid
  was checked.
- **Fix:**
  - (runner) `pid_state()` reads `ProcessLookupError` as dead, `PermissionError` as alive and
    anything else as unknown. `run_liveness()` combines the manager's pid, the session's own pid
    and whether `log.md` or `progress.md` was written in the last 10 minutes. Any live signal is
    alive, and only a positive dead reading may mark a run orphaned.
  - (supervisor) Before a retry or a hand resume, it checks the previous attempt. If that may
    still be working, the stage shows `waiting-previous` and the next pass checks again. Then it
    takes `worktree.lock` in the pipeline directory with `O_EXCL`, naming the new attempt. The
    run releases the lock when it ends, and each supervisor pass releases a stale one.

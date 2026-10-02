#!/usr/bin/env python3
"""bp_config.py - every path and setting the pipeline reads, from environment variables.

Nothing here is hard-coded to one machine. Each value has a default that works on a fresh
install, and `.env.example` at the repo root lists them all. Read once at import; the tests
point the paths at a temporary directory by setting attributes on the modules that use them.

    BUILD_PIPELINE_HOME         state directory (builds, runs, queue, reports)   ~/.build-pipeline
    BUILD_PIPELINE_WORKTREES    where each build's git worktree is created        ~/dev
    BUILD_PIPELINE_TZ           IANA time zone for queue times                    the machine's zone
    BUILD_PIPELINE_MODEL        model passed to `claude --model`                  opus
    BUILD_PIPELINE_OWNER        how briefs name the person who asked              the owner
    BUILD_PIPELINE_RUNNER       module that launches and watches runs             bp_runner
    BUILD_PIPELINE_CLAUDE       the Claude Code CLI                               claude
    BUILD_PIPELINE_CLAUDE_ARGS  extra CLI arguments for every run                 --permission-mode acceptEdits
    BUILD_PIPELINE_MAX_RUNS     managed runs at once                              3
    BUILD_PIPELINE_THRESHOLD    5-hour usage percentage that holds new stages     90
    BUILD_PIPELINE_WEEKLY       7-day usage percentage that holds new stages      95
    BUILD_PIPELINE_PROTECTED    comma list of paths stages may not edit           .claude/,CLAUDE.md
    BUILD_PIPELINE_STYLE_LINE   the house-style sentence in every brief           plain words, no em dashes
    BUILD_PIPELINE_STYLE_GUIDE  optional writing guide with a HOUSE-BLOCK         (none)
    BUILD_PIPELINE_STYLE_CHECK  optional command run on each report, {file} slot  (none)
    BUILD_PIPELINE_NOTIFY       optional command given a title and a note         (log file only)

Stdlib only. Python 3.9 or later.
"""
from __future__ import annotations

import os
import re
import zoneinfo
from pathlib import Path


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return v.strip() if v and v.strip() else default


def _path(name: str, default: Path) -> Path:
    return Path(_env(name, str(default))).expanduser()


HOME = _path("BUILD_PIPELINE_HOME", Path.home() / ".build-pipeline")
WORKTREES = _path("BUILD_PIPELINE_WORKTREES", Path.home() / "dev")
MODEL = _env("BUILD_PIPELINE_MODEL", "opus")
OWNER = _env("BUILD_PIPELINE_OWNER", "the owner")
RUNNER = _env("BUILD_PIPELINE_RUNNER", "bp_runner")
CLAUDE = _env("BUILD_PIPELINE_CLAUDE", "claude")
CLAUDE_ARGS = _env("BUILD_PIPELINE_CLAUDE_ARGS", "--permission-mode acceptEdits")
MAX_RUNS = int(_env("BUILD_PIPELINE_MAX_RUNS", "3"))
THRESHOLD = float(_env("BUILD_PIPELINE_THRESHOLD", "90"))
WEEKLY = float(_env("BUILD_PIPELINE_WEEKLY", "95"))
PROTECTED = [x.strip() for x in _env("BUILD_PIPELINE_PROTECTED", ".claude/,CLAUDE.md").split(",") if x.strip()]
DEFAULT_STYLE_LINE = ('Write in plain words, use a full stop, comma or brackets where an em dash would go, '
                      'and join lists with commas or "and".')
STYLE_LINE = _env("BUILD_PIPELINE_STYLE_LINE", DEFAULT_STYLE_LINE)
STYLE_GUIDE = _env("BUILD_PIPELINE_STYLE_GUIDE")
STYLE_CHECK = _env("BUILD_PIPELINE_STYLE_CHECK")
NOTIFY = _env("BUILD_PIPELINE_NOTIFY")

BUILDS = HOME / "builds"
RUNS = HOME / "runs"
REPORTS = HOME / "reports"
QUEUE = HOME / "queue"
STATE = HOME / "state"


def local_zone() -> zoneinfo.ZoneInfo:
    """BUILD_PIPELINE_TZ, else TZ, else the zone /etc/localtime links to, else UTC.

    A real ZoneInfo, not a fixed offset: an offset frozen at today's value queues a time read on
    the night the clocks change an hour early or late."""
    for name in (_env("BUILD_PIPELINE_TZ"), _env("TZ").lstrip(":")):
        if name:
            try:
                return zoneinfo.ZoneInfo(name)
            except (zoneinfo.ZoneInfoNotFoundError, ValueError):
                pass
    try:
        target = os.path.realpath("/etc/localtime")
        m = re.search(r"zoneinfo/(.+)$", target)
        if m:
            return zoneinfo.ZoneInfo(m.group(1))
    except (OSError, zoneinfo.ZoneInfoNotFoundError, ValueError):
        pass
    return zoneinfo.ZoneInfo("UTC")


TZ = local_zone()


def style_block() -> str:
    """The text between `<!-- HOUSE-BLOCK start -->` and `<!-- HOUSE-BLOCK end -->` in the style
    guide, or "" when no guide is configured or the marker is missing. A brief never fails over it."""
    if not STYLE_GUIDE:
        return ""
    try:
        text = Path(STYLE_GUIDE).expanduser().read_text("utf-8", "ignore")
    except OSError:
        return ""
    m = re.search(r"<!-- HOUSE-BLOCK start -->\n(.*?)<!-- HOUSE-BLOCK end -->", text, re.DOTALL)
    return m.group(1).rstrip("\n") if m else ""

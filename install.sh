#!/usr/bin/env bash
# install.sh: set up the /build pipeline on this machine.
#
#   ./install.sh                 check requirements, create the state directory, install the skill
#   ./install.sh --launchd       ...and run `tick` every minute with a macOS LaunchAgent
#   ./install.sh --cron          ...and print the crontab line that does the same on Linux
#   ./install.sh --force         replace an existing ~/.claude/skills/build/SKILL.md
#   ./install.sh --uninstall     remove the skill and the LaunchAgent (state is kept)
#
# Settings come from .env in this directory (copy .env.example). Nothing outside
# ~/.claude/skills/build, the state directory and ~/Library/LaunchAgents is written.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABEL="com.github.claude-build-pipeline.tick"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
SKILL_DIR="${CLAUDE_SKILLS_DIR:-$HOME/.claude/skills}/build"
LAUNCHD=0; CRON=0; FORCE=0; UNINSTALL=0
for a in "$@"; do
  case "$a" in
    --launchd) LAUNCHD=1 ;;
    --cron) CRON=1 ;;
    --force) FORCE=1 ;;
    --uninstall) UNINSTALL=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "install.sh: unknown option $a" >&2; exit 2 ;;
  esac
done

if [ -f "$REPO/.env" ]; then set -a; . "$REPO/.env"; set +a; fi
STATE="${BUILD_PIPELINE_HOME:-$HOME/.build-pipeline}"
STATE="${STATE/#\~/$HOME}"

if [ "$UNINSTALL" = 1 ]; then
  if [ -f "$PLIST" ]; then launchctl unload "$PLIST" 2>/dev/null || true; rm -f "$PLIST"; echo "removed $PLIST"; fi
  if [ -f "$SKILL_DIR/SKILL.md" ] && grep -q "build_pipeline.py" "$SKILL_DIR/SKILL.md"; then
    rm -f "$SKILL_DIR/SKILL.md"; rmdir "$SKILL_DIR" 2>/dev/null || true; echo "removed the /build skill"
  fi
  echo "state kept in $STATE"
  exit 0
fi

ok=1
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)' 2>/dev/null \
  && echo "python3: $(python3 --version 2>&1)" || { echo "python3 3.9 or later is required" >&2; ok=0; }
command -v git >/dev/null && echo "git: $(git --version)" || { echo "git is required" >&2; ok=0; }
CLAUDE_BIN="${BUILD_PIPELINE_CLAUDE:-claude}"
if command -v "$CLAUDE_BIN" >/dev/null || [ -x "$HOME/.local/bin/claude" ]; then
  echo "claude: found"
else
  echo "claude: NOT FOUND. Install Claude Code (https://docs.anthropic.com/en/docs/claude-code) before queueing a build." >&2
fi
[ "$ok" = 1 ] || exit 1

mkdir -p "$STATE"/{builds,runs,reports,queue/pending,queue/done,state}
echo "state directory: $STATE"

mkdir -p "$SKILL_DIR"
if [ -f "$SKILL_DIR/SKILL.md" ] && [ "$FORCE" != 1 ] && ! grep -q "$REPO/src" "$SKILL_DIR/SKILL.md"; then
  echo "a different /build skill is already at $SKILL_DIR/SKILL.md; rerun with --force to replace it" >&2
  exit 1
fi
sed -e "s#{{BP_SRC}}#$REPO/src#g" -e "s#{{BP_DOCS}}#$REPO/docs#g" "$REPO/skill/build/SKILL.md" > "$SKILL_DIR/SKILL.md"
echo "skill installed: $SKILL_DIR/SKILL.md (type /build in Claude Code)"

if [ "$LAUNCHD" = 1 ]; then
  [ "$(uname)" = "Darwin" ] || { echo "--launchd is for macOS; use --cron elsewhere" >&2; exit 1; }
  mkdir -p "$HOME/Library/LaunchAgents"
  sed -e "s#{{LABEL}}#$LABEL#g" -e "s#{{REPO}}#$REPO#g" -e "s#{{HOME_DIR}}#$STATE#g" \
    "$REPO/launchd/tick.plist.template" > "$PLIST"
  launchctl unload "$PLIST" 2>/dev/null || true
  launchctl load "$PLIST"
  echo "LaunchAgent loaded: $PLIST (tick every 60 s, log in $STATE/state/tick.log)"
fi
if [ "$CRON" = 1 ]; then
  echo "add this line with 'crontab -e':"
  echo "* * * * * /bin/bash \"$REPO/bin/build-pipeline\" tick >> \"$STATE/state/tick.log\" 2>&1"
fi
echo "done. Try: $REPO/bin/build-pipeline plan --request \"Add a --version flag\" --repo /path/to/a/git/repo"

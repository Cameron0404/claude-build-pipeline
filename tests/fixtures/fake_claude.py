#!/usr/bin/env python3
"""A stand-in for the `claude` CLI, for the runner tests. Reads the brief on stdin, writes the
stage output and report the brief names (with every section the supervisor checks), and prints
stream-json events as `claude -p --output-format stream-json` would.

FAKE_CLAUDE_MODE: ok (default) | auth (not logged in) | network (an API error) | fail (exit 1)
"""
import json
import os
import re
import sys
import uuid

brief = sys.stdin.read()
mode = os.environ.get("FAKE_CLAUDE_MODE", "ok")
sid = str(uuid.uuid4())
print(json.dumps({"type": "system", "subtype": "init", "session_id": sid}), flush=True)
if mode == "auth":
    print(json.dumps({"type": "result", "is_error": True, "result": "Not logged in · Please run /login", "session_id": sid}))
    sys.exit(1)
if mode == "network":
    print(json.dumps({"type": "result", "is_error": True, "result": "API Error: ECONNRESET", "session_id": sid}))
    sys.exit(1)
if mode == "fail":
    sys.exit(1)

out = re.search(r"\*\*Output:\*\* `([^`]+)`", brief)
rep = re.search(r"\*\*Report:\*\* `([^`]+)`", brief)
kind = re.search(r"^## This stage: (\w+)", brief, re.M)
notes = sorted(set(re.findall(r"## Owner's note (N\d+)", brief)))
if out and rep:
    k = kind.group(1) if kind else ""
    body = ["# Stage output", "", "Findings written as they were made.", "",
            "## Depth justification", "A test stand-in: the work is a fixture.", ""]
    if k == "the":                                   # "## This stage: the plan"
        body += ["## Human gates", "- none", ""]
    if k in ("build", "compile"):
        body += ["## Verdict", "fake suite, exit 0, 3 pass 0 fail", "suite: pass", ""]
    if k == "compile":
        body += ["## Delivered", "delivered: yes, the fixture", ""]
    body += [f"{n}: handled in this stage." for n in notes]
    body += ["", "STAGE COMPLETE"]
    os.makedirs(os.path.dirname(out.group(1)), exist_ok=True)
    os.makedirs(os.path.dirname(rep.group(1)), exist_ok=True)
    open(out.group(1), "w").write("\n".join(body) + "\n")
    open(rep.group(1), "w").write("Report: the fixture did its stage.\n")
print(json.dumps({"type": "result", "subtype": "success", "is_error": False,
                  "result": "Done. Output written.", "session_id": sid, "total_cost_usd": 0}))

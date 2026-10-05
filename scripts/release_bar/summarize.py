"""Summarize a release-bar run: every step's verdict and every failed scenario.

    summarize.py EVIDENCE_DIR          the whole run; writes EVIDENCE_DIR/summary.md
    summarize.py --phase CHAOS_DIR     one line for one chaos phase

Reads what `run.sh` wrote: `steps.tsv` (each step's exit status), each chaos
phase's `results.json` and `isolation-failures.json`, and the restart test's
`summary.json`. Exit 0 only when every step that ran passed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def chaos_phase(directory: Path) -> tuple[str, list[str]]:
    """One line for a chaos phase, and a line per failed scenario or isolation failure."""
    results = _load(directory / "results.json", None)
    partial = results is None
    if partial:
        results = _load(directory / "results.partial.json", [])
    isolation = _load(directory / "isolation-failures.json", [])
    passed = sum(1 for item in results if item.get("passed"))
    line = f"{passed}/{len(results)} scenarios passed, {len(isolation)} isolation failures" + (
        " (the phase did not finish)" if partial else ""
    )
    details = [
        f"{item.get('scenario')} {item.get('round')}: "
        f"{item.get('error') or (item.get('terminal') or {}).get('state')} "
        f"{str(item.get('detail') or (item.get('terminal') or {}).get('failure') or '')[:200]}"
        for item in results
        if not item.get("passed")
    ]
    details += [
        f"isolation: {item.get('round')} was {item.get('state')} during {item.get('context')}"
        for item in isolation
    ]
    return line, details


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--phase", action="store_true")
    args = parser.parse_args()
    if args.phase:
        line, details = chaos_phase(args.evidence)
        print("; ".join([line, *details[:3]]))
        return 0

    evidence: Path = args.evidence
    steps = []
    for raw in (evidence / "steps.tsv").read_text(encoding="utf-8").splitlines():
        name, code, started, finished = raw.split("\t")
        steps.append((name, int(code), started, finished))
    lines = [f"# Release bar: {evidence.name}", "", "| Step | Result | Started | Finished |"]
    lines.append("|---|---|---|---|")
    failures: list[str] = []
    total = passed_total = 0
    for name, code, started, finished in steps:
        result = "passed" if code == 0 else f"FAILED (exit {code})"
        if name.startswith("chaos-"):
            line, details = chaos_phase(evidence / "chaos" / name.removeprefix("chaos-"))
            result += f": {line}"
            results = _load(evidence / "chaos" / name.removeprefix("chaos-") / "results.json", [])
            total += len(results)
            passed_total += sum(1 for item in results if item.get("passed"))
            failures += [f"{name}: {detail}" for detail in details]
        elif name in {"restart", "crash"} or name.startswith("crash-"):
            summary = _load(evidence / name / "summary.json", {})
            heal = summary.get("ready_after_restart_s") or {}
            if heal:
                result += ": READY again after " + ", ".join(
                    f"{round_id} {seconds}s" for round_id, seconds in sorted(heal.items())
                )
        if code != 0 and not name.startswith("chaos-"):
            failures.append(f"{name}: see logs/{name}.log")
        lines.append(f"| {name} | {result} | {started} | {finished} |")
    lines += ["", f"Chaos scenarios: {passed_total}/{total} passed.", ""]
    ok = bool(steps) and all(code == 0 for _, code, _, _ in steps)
    lines.append("**PASSED**" if ok else "**FAILED**")
    if failures:
        lines += ["", "## Failures", ""] + [f"- {failure}" for failure in failures]
    text = "\n".join(lines) + "\n"
    (evidence / "summary.md").write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

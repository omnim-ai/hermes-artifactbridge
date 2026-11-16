"""A stand-in ``omp-run`` executable for the agent-run tests (the plugin runs it as a real subprocess).

``FakeOmpRun(directory)`` writes an executable wrapper there. Each call appends one record to ``calls.jsonl``:
its argv and, for ``start``, the request file's mode, type, owner and content as omp-run would read them.
``scenario.json`` says what each subcommand prints and exits with (the shapes of artifact-forge
infra/cloudflare-agent-machine/container/omp-run, AI-6424); a step with ``block`` waits until ``release()``.
"""
from __future__ import annotations

import json
import os
import stat
import sys
import time
from pathlib import Path

UNIT = "omp-agent-agd1862633374-1"
STARTED = {"ok": True, "unit": UNIT, "run_key": "AGD-1862633374", "phase": "running", "runner": "cloudflare-container"}
BLOCK_BOUND = 30.0


def completed(text: str = "done") -> dict:
    return {"unit": UNIT, "active": False, "outcome": "completed", "result_text": text, "steps": 3, "wall_s": 41,
            "usage": {"input": 10, "cache_read": 0, "cache_write": 0, "output": 5}}


DEFAULT = {"start": {"exit": 0, "answer": STARTED}, "wait": {"exit": 0, "answer": completed()}, "stop": {"exit": 0}}


class FakeOmpRun:
    def __init__(self, directory: Path):
        self.dir = directory
        self.binary = str(directory / "omp-run")
        Path(self.binary).write_text(f'#!/bin/sh\nexec "{sys.executable}" "{__file__}" "{directory}" "$@"\n')
        os.chmod(self.binary, 0o755)
        self.set()

    def set(self, **steps: dict) -> None:
        """Each subcommand's ``{"exit", "answer"?, "block"?}``; the rest keep their defaults."""
        (self.dir / "scenario.json").write_text(json.dumps({**DEFAULT, **steps}))

    def release(self) -> None:
        (self.dir / "release").touch()

    @property
    def calls(self) -> list[dict]:
        path = self.dir / "calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def argvs(self, command: str) -> list[list[str]]:
        return [c["argv"] for c in self.calls if c["argv"][:1] == [command]]


def main(directory: str, argv: list[str]) -> int:
    state = Path(directory)
    call: dict = {"argv": argv}
    if argv[:1] == ["start"] and "--request" in argv:
        path = argv[argv.index("--request") + 1]
        info = os.lstat(path)
        call.update(path=path, mode=stat.S_IMODE(info.st_mode), regular=stat.S_ISREG(info.st_mode),
                    owner=info.st_uid == os.getuid(), request=json.loads(Path(path).read_text(encoding="utf-8")))
    with open(state / "calls.jsonl", "a", encoding="utf-8") as log:
        log.write(json.dumps(call) + "\n")
    step = json.loads((state / "scenario.json").read_text()).get(argv[0], {}) if argv else {}
    deadline = time.monotonic() + BLOCK_BOUND
    while step.get("block") and not (state / "release").exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    if "answer" in step:
        print(json.dumps(step["answer"]))
    return step.get("exit", 0)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2:]))

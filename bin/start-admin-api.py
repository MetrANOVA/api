#!/usr/bin/env python3
from pathlib import Path
from subprocess import run


def main() -> int:
	repo_root = Path(__file__).resolve().parent.parent
	result = run(["uv", "run", "--package", "admin_api", "admin_api"], cwd=repo_root)
	return result.returncode


if __name__ == "__main__":
	raise SystemExit(main())

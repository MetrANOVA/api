#!/usr/bin/env python3
from pathlib import Path
from subprocess import run


def run_suite(name: str, project_root: Path, test_path: str) -> int:
    command = ["uv", "run", "--all-groups", "pytest", test_path, "-q"]
    print(f"\n==> Running {name} tests")
    result = run(command, cwd=project_root)
    return result.returncode


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent

    suites = [
        ("metranova", repo_root / "packages" / "metranova", "tests"),
        ("admin_api", repo_root, "tests/admin_api"),
        ("pipeline", repo_root / "packages" / "pipeline", "test"),
    ]

    for name, project_root, test_path in suites:
        exit_code = run_suite(name, project_root, test_path)
        if exit_code != 0:
            return exit_code

    print("\nAll package tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

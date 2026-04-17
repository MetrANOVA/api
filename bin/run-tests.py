#!/usr/bin/env python3
from pathlib import Path
from subprocess import run


def run_suite(repo_root: Path, package: str, test_path: str) -> int:
    command = [
        "uv",
        "run",
        # "--group",
        # "dev",
        "--package",
        package,
        "pytest",
        test_path,
        "-q",
    ]
    print(f"\n==> Running {package} tests")
    result = run(command, cwd=repo_root)
    return result.returncode


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent

    suites = [
        ("metranova", "tests/metranova_core"),
        ("admin_api", "packages/admin_api/tests"),
        # ("pipeline", "packages/pipeline/tests"),
    ]

    for package, test_path in suites:
        exit_code = run_suite(repo_root, package, test_path)
        if exit_code != 0:
            return exit_code

    print("\nAll package tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

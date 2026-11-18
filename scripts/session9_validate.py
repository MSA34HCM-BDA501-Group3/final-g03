"""Run final checks in the pinned Docker runtime and retain their complete logs."""

import json
from pathlib import Path
import subprocess


def main():
    root = Path("evidence/session9")
    root.mkdir(parents=True, exist_ok=True)
    files = [
        "app.py",
        "baseline.py",
        "pipeline.py",
        "producer.py",
        "starter_stream.py",
        "spark_engine.py",
        "spark_batch.py",
        "traffic_online.py",
        "traffic_data",
        "tests",
        "scripts/build_proposal_pdf.py",
        "scripts/build_slides.py",
    ]
    files += [p.as_posix() for p in Path("scripts").glob("session9_*.py")]
    commands = {
        "lint": [
            "python",
            "-m",
            "ruff",
            "check",
            "--select",
            "E,F,W,C901",
            "--config",
            "lint.mccabe.max-complexity=14",
            "--config",
            "line-length=120",
            *files,
        ],
        "full_suite": ["python", "-m", "pytest", "-q"],
    }
    results = {}
    for name, command in commands.items():
        argv = [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "-e",
            "KAFKA_BOOTSTRAP_SERVERS=kafka:29092",
            "spark",
            *command,
        ]
        process = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600)
        (root / f"{name}.log").write_bytes(process.stdout)
        results[name] = dict(command=argv, exit_code=process.returncode)
        print(f"{name}: exit {process.returncode}", flush=True)
    (root / "validation.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    if any(r["exit_code"] for r in results.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

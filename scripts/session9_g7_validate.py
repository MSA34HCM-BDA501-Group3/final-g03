"""Host orchestration for G7 render/content checks using installed skill helpers."""

import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent


def skill_file(package, relative):
    base = Path.home() / ".codex/plugins/cache/openai-primary-runtime" / package
    found = list(base.glob("*/skills/" + package + "/" + relative))
    if not found:
        raise FileNotFoundError(f"Installed {package} skill helper unavailable: {relative}")
    return max(found, key=lambda p: p.stat().st_mtime)


def main():
    docs = skill_file("documents", "render_docx.py").parent
    slides = skill_file("presentations", "container_tools/inspect_presentation_package_integrity.py").parent.parent
    base = [
        "docker",
        "run",
        "--rm",
        "--mount",
        f"type=bind,source={ROOT},target=/workspace",
        "--mount",
        f"type=bind,source={docs},target=/skills/documents,readonly",
        "--mount",
        f"type=bind,source={slides},target=/skills/presentations,readonly",
    ]
    fonts = Path("C:/Windows/Fonts")
    if fonts.is_dir():
        base += ["--mount", f"type=bind,source={fonts},target=/usr/local/share/fonts/windows,readonly"]
    commands = [
        [
            "python",
            "/skills/documents/render_docx.py",
            "BDA501_Session9_Lab_Manual_G03.docx",
            "--output_dir",
            "tmp/g7/manual",
            "--emit_pdf",
        ],
        ["python", "-m", "scripts.session9_render_artifacts"],
        ["python", "-m", "scripts.session9_artifact_checks"],
    ]
    results = []
    logs = []
    for command in commands:
        argv = [*base, "final-g03-artifacts", *command]
        process = subprocess.run(argv, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=300)
        logs.extend(["COMMAND: " + repr(argv), process.stdout, process.stderr])
        results.append(dict(command=argv, exit_code=process.returncode))
        if process.returncode:
            break
    root = ROOT / "evidence/session9"
    (root / "g7_validation.log").write_text("\n".join(logs), encoding="utf-8")
    (root / "g7_validation.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    if any(result["exit_code"] for result in results):
        print("\n".join(logs))
        raise SystemExit(1)
    print("G7 render and structural/content checks passed; visual review remains a separate step.")


if __name__ == "__main__":
    main()

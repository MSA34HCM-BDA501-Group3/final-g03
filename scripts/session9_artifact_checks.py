"""G7 structural/content checks against actual exported artifacts and source evidence."""

import json
from pathlib import Path
import subprocess
import sys
from zipfile import ZipFile
import xml.etree.ElementTree as ET

import fitz
import pandas as pd
from pptx import Presentation

ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = ROOT / "evidence/session9"


def main():
    report = fitz.open(ROOT / "HCMC_Smart_Traffic_Final_Report.pdf")
    slides_pdf = fitz.open(ROOT / "HCMC_Smart_Traffic_Final_Slides.pdf")
    deck = Presentation(ROOT / "HCMC_Smart_Traffic_Final_Presentation.pptx")
    report_text = "\n".join(p.get_text() for p in report)
    deck_text = "\n".join(p.get_text() for p in slides_pdf)
    assert len(report) == 16 and len(slides_pdf) == len(deck.slides) == 16
    assert "Session 9 Technical Readiness Summary" in deck_text
    assert "UNVERIFIED" in report_text and "UNVERIFIED" in deck_text
    assert "weighted-F1 0.9439" in report_text and "severe F1 0.9409" in report_text
    assert "lag cells" not in report_text and "lag cells" not in deck_text
    assert "downstream candidate" in report_text
    ablation = pd.read_csv(ROOT / "outputs/ablation_results.csv")
    rf = ablation[ablation.model.str.startswith("Random Forest (+Graph")].iloc[0]
    dt = ablation[ablation.model.str.startswith("Decision Tree (+Graph")].iloc[0]
    assert float(rf.weighted_f1_6class) == 0.9439
    assert float(rf.severe_ef_f1) == 0.9409
    assert float(dt.severe_pr_auc) == 0.9327
    missing = []
    with ZipFile(ROOT / "BDA501_Session9_Lab_Manual_G03.docx") as z:
        document = ET.fromstring(z.read("word/document.xml"))
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        text = " ".join(t.text or "" for t in document.findall(".//w:t", ns))
        assert "Historical availability" in text and "UNVERIFIED" in text
        tables = document.findall(".//w:tbl", ns)
        assert any(len(t.findall("w:tr", ns)) == 17 for t in tables), "16-item checklist absent"
        assert any(len(t.findall("w:tr", ns)) == 10 for t in tables), "9-row red flags absent"
        rels = ET.fromstring(z.read("word/_rels/document.xml.rels"))
        targets = [r.attrib["Target"] for r in rels if r.attrib.get("Type", "").endswith("/hyperlink")]
        missing = [target for target in targets if not (ROOT / target).exists()]
        # Word reuses one relationship for repeated targets: count linked cells,
        # rather than requiring 16 distinct destinations for the 16-item checklist.
        hyperlinks = len(document.findall(".//w:hyperlink", ns))
        assert hyperlinks >= 25 and targets and not missing, (hyperlinks, missing)
    outside = []
    for number, slide in enumerate(deck.slides, 1):
        for shape in slide.shapes:
            if shape.left < 0 or shape.top < 0 or shape.left + shape.width > deck.slide_width + 1000:
                outside.append([number, shape.name])
            if shape.top + shape.height > deck.slide_height + 1000:
                outside.append([number, shape.name])
    assert not outside, outside
    package = subprocess.run(
        [
            sys.executable,
            "/skills/presentations/container_tools/inspect_presentation_package_integrity.py",
            str(ROOT / "HCMC_Smart_Traffic_Final_Presentation.pptx"),
            "--fail-on-findings",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    result = dict(
        status="pass",
        report_pages=len(report),
        slides=len(deck.slides),
        manual_pages=len(list((ROOT / "tmp/g7/manual").glob("page-*.png"))),
        checklist_items=16,
        red_flags=9,
        manual_hyperlinks=hyperlinks,
        unique_manual_destinations=len(targets),
        missing_links=missing,
        pptx_outside_shapes=outside,
        package_integrity=json.loads(package.stdout),
        content_checks="RF/DT metrics match owning CSV; candidate-window denominator explicit; limits preserved",
        limitation=("Structural/content checks supplement visual review "
                    "and do not establish PowerPoint native execution."),
    )
    (EVIDENCE / "g7_artifact_checks.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()

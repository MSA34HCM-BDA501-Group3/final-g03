"""Render the real final PPTX, PDF and manual to page PNGs for G7 visual review."""

from pathlib import Path
import json
import shutil
import subprocess

import fitz
from PIL import Image, ImageDraw

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "tmp/g7"


def render_pdf(path, directory):
    directory.mkdir(parents=True, exist_ok=True)
    document = fitz.open(path)
    pages = []
    for number, page in enumerate(document, 1):
        pix = page.get_pixmap(matrix=fitz.Matrix(1.6, 1.6))
        image_path = directory / f"page-{number}.png"
        pix.save(image_path)
        pages.append(
            dict(
                page=number,
                text_characters=len(page.get_text()),
                width=page.rect.width,
                height=page.rect.height,
                png=str(image_path.relative_to(ROOT)),
            )
        )
    return pages


def montage(directory, label):
    paths = sorted(directory.glob("page-*.png"), key=lambda p: int(p.stem.split("-")[-1]))
    width = 320
    height = 210 if label == "slides" else 480
    for batch in range(0, len(paths), 8):
        subset = paths[batch : batch + 8]
        grid = Image.new("RGB", (width * 4, height * 2), "#DDE5ED")
        draw = ImageDraw.Draw(grid)
        for index, path in enumerate(subset):
            image = Image.open(path).convert("RGB")
            image.thumbnail((width - 10, height - 30))
            x = index % 4 * width
            y = index // 4 * height
            grid.paste(image, (x + (width - image.width) // 2, y + 24))
            draw.text((x + 8, y + 5), f"{label} {path.stem}", fill="black")
        grid.save(BUILD / f"{label}_montage_{batch // 8 + 1}.png")


def main():
    converted = BUILD / "converted"
    converted.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "soffice",
            "-env:UserInstallation=file:///tmp/lo-g7-slides",
            "--headless",
            "--convert-to",
            "pdf",
            "--outdir",
            str(converted),
            str(ROOT / "HCMC_Smart_Traffic_Final_Presentation.pptx"),
        ],
        check=True,
    )
    shutil.copy2(converted / "HCMC_Smart_Traffic_Final_Presentation.pdf", ROOT / "HCMC_Smart_Traffic_Final_Slides.pdf")
    result = {
        "report": render_pdf(ROOT / "HCMC_Smart_Traffic_Final_Report.pdf", BUILD / "report"),
        "slides": render_pdf(ROOT / "HCMC_Smart_Traffic_Final_Slides.pdf", BUILD / "slides"),
    }
    manual = BUILD / "manual"
    result["manual"] = [
        dict(page=int(p.stem.split("-")[-1]), png=str(p.relative_to(ROOT)))
        for p in sorted(manual.glob("page-*.png"), key=lambda p: int(p.stem.split("-")[-1]))
    ]
    for name in result:
        montage(BUILD / name, name)
    (BUILD / "render_inventory.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: len(v) for k, v in result.items()}))


if __name__ == "__main__":
    main()

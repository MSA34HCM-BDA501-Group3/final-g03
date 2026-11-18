"""Add the current claim boundary to editable report/script sources, preserving history."""

from pathlib import Path


NOTE = """<!-- session9-current-status -->
> **Session 9 implementation status, 18 November 2026:** The current handoff is
> `{evidence}/README.md`, with commands, provenance and gate limitations. Historical
> replay now has a Python Kafka consumer, journaled Mongo ingress, exact deduplication,
> immutable forecast revisions and a run-scoped dashboard. The prior file-stream,
> local replay and broker publish checks remain separate historical experiments.
>
> Preserved batch measurements and new replay measurements retain separate provenance.
> The 687.43x and 1.023x statistics compare different populations and do
> not demonstrate reduced task skew. Warm Mongo query latency is separate from
> end-to-end latency. Next-slot prediction does not establish 30 minutes of advance
> warning before target start. Coverage of cold-start roads does not establish accuracy.
> Historical point-in-time availability remains UNVERIFIED. Current DOCX/PDF/PPTX
> build and visual-review status is recorded in `{evidence}/artifact_review.json`.
> G7 handoff approval does not establish all gates operationally Ready or a verified grade.
<!-- /session9-current-status -->

"""


def main():
    for name in ("README.md", "TECHNICAL_NOTE.md", "PRESENTATION_SCRIPT.md", "product/00-proposal/project-proposal.md"):
        path = Path(name)
        content = path.read_text(encoding="utf-8-sig")
        if "<!-- session9-current-status -->" in content:
            content = content.split("<!-- /session9-current-status -->", 1)[1].lstrip()
        evidence = "../../evidence/session9" if "product/" in name else "evidence/session9"
        path.write_text(NOTE.format(evidence=evidence) + content, encoding="utf-8")


if __name__ == "__main__":
    main()

"""Render the incident table into a dark HTML summary at out/summary.html."""

from __future__ import annotations

from pathlib import Path
from statistics import median
from typing import Literal

from jinja2 import Environment, FileSystemLoader, select_autoescape
from pydantic import BaseModel

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = REPO_ROOT / "templates"
OUT_DIR = REPO_ROOT / "out"

_ENV = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR), autoescape=select_autoescape(["html"])
)


class Incident(BaseModel):
    number: int
    job: str
    failure: str
    root_cause: str
    status: str
    status_kind: Literal["verified", "suggested", "failed"]


def render_summary(incidents: list[Incident], diagnose_seconds: list[float]) -> str:
    kpis = {
        "diagnosed": len(incidents),
        "verified": sum(1 for i in incidents if i.status_kind == "verified"),
        "prs_proposed": sum(1 for i in incidents if i.status_kind != "suggested"),
        "median_diagnose": f"{median(diagnose_seconds):.1f}s" if diagnose_seconds else "n/a",
    }
    subtitle = f"Seeded demo run · {len(incidents)} incidents · Muse Spark via Meta Model API"
    template = _ENV.get_template("summary.html.j2")
    return template.render(incidents=incidents, kpis=kpis, subtitle=subtitle)


def write_summary(
    incidents: list[Incident], diagnose_seconds: list[float], out_path: Path = OUT_DIR / "summary.html"
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(render_summary(incidents, diagnose_seconds))
    return out_path

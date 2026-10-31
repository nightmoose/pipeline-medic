"""The only module allowed to call the Meta Model API (Muse Spark)."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Literal

from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError

REPO_ROOT = Path(__file__).resolve().parent.parent
BASE_URL = "https://api.meta.ai/v1"
MODEL = os.environ.get("MODEL", "muse-spark-1.3")
CACHE_DIR = REPO_ROOT / ".cache" / "llm"

SYSTEM_PROMPT = (
    "You are Pipeline Medic, an on-call engineer for dbt models orchestrated by "
    "Airflow. You are given evidence about a failing dbt node: the failing "
    "Airflow task, the dbt check that failed, the error message, the model's "
    "SQL, and observed row/null stats. Reply with JSON matching the Diagnosis "
    "schema: root_cause (string), evidence (list of short strings), confidence "
    "(float between 0 and 1), patch (a unified diff fixing the dbt project, or "
    "an empty string if no code change can fix it), and action ('fix' if patch "
    "resolves it, or 'suggest' if this is a data-quality problem upstream that "
    "code cannot fix)."
)


class Diagnosis(BaseModel):
    root_cause: str
    evidence: list[str] = Field(default_factory=list)
    confidence: float
    patch: str = ""
    action: Literal["fix", "suggest"]


def diagnose(context: str, *, mock_response: dict | None = None) -> Diagnosis:
    """Diagnose a dbt failure described by `context`.

    Pass `mock_response` (a dict matching `Diagnosis`) to skip the API call
    entirely, e.g. for `--mock` runs or offline tests.
    """
    if mock_response is not None:
        return Diagnosis.model_validate(mock_response)

    prompt = f"Evidence:\n{context}"
    cache_path = _cache_path(prompt)
    if cache_path.exists():
        return Diagnosis.model_validate_json(cache_path.read_text())

    client = OpenAI(base_url=BASE_URL, api_key=os.environ["MODEL_API_KEY"])
    raw = _complete(client, prompt)
    try:
        diagnosis = Diagnosis.model_validate_json(raw)
    except ValidationError:
        raw = _complete(
            client,
            prompt
            + "\n\nYour previous reply was not valid JSON for the Diagnosis "
            "schema. Reply again with valid JSON only, no commentary.",
        )
        diagnosis = Diagnosis.model_validate_json(raw)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(diagnosis.model_dump_json())
    return diagnosis


def _complete(client: OpenAI, prompt: str) -> str:
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "Diagnosis", "schema": Diagnosis.model_json_schema()},
        },
    )
    return response.choices[0].message.content


def _cache_path(prompt: str) -> Path:
    digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{digest}.json"

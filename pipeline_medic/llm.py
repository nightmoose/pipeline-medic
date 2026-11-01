"""The only module allowed to call the Meta Model API (Muse Spark)."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError

REPO_ROOT = Path(__file__).resolve().parent.parent
BASE_URL = "https://api.meta.ai/v1"
MODEL = os.environ.get("MODEL", "muse-spark-1.3")
CACHE_DIR = REPO_ROOT / ".cache" / "llm"
REQUEST_TIMEOUT = 120
MAX_LLM_CALLS = int(os.environ.get("MAX_LLM_CALLS", "20"))
INPUT_PRICE_PER_TOKEN = 1.25 / 1_000_000
OUTPUT_PRICE_PER_TOKEN = 4.25 / 1_000_000


class LLMCallCapError(RuntimeError):
    """Raised when MAX_LLM_CALLS real Meta Model API calls have already been made."""

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


def diagnose(
    context: str, *, mock_response: dict | None = None, scenario: str | None = None
) -> Diagnosis:
    """Diagnose a dbt failure described by `context`.

    Pass `mock_response` (a dict matching `Diagnosis`) to skip the API call
    entirely, e.g. for `--mock` runs or offline tests. `scenario`, if given,
    is only used to label the usage log line.
    """
    if mock_response is not None:
        return Diagnosis.model_validate(mock_response)

    prompt = f"Evidence:\n{context}"
    cache_path = _cache_path(prompt)
    if cache_path.exists():
        return Diagnosis.model_validate_json(cache_path.read_text())

    client = OpenAI(
        base_url=BASE_URL,
        api_key=os.environ["MODEL_API_KEY"],
        max_retries=0,
        timeout=REQUEST_TIMEOUT,
    )
    raw = _complete(client, prompt, scenario=scenario)
    try:
        diagnosis = Diagnosis.model_validate_json(raw)
    except ValidationError:
        raw = _complete(
            client,
            prompt
            + "\n\nYour previous reply was not valid JSON for the Diagnosis "
            "schema. Reply again with valid JSON only, no commentary.",
            scenario=scenario,
        )
        diagnosis = Diagnosis.model_validate_json(raw)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(diagnosis.model_dump_json())
    return diagnosis


def _complete(client: OpenAI, prompt: str, *, scenario: str | None) -> str:
    """Make one real Meta Model API call. Counts against MAX_LLM_CALLS and
    logs usage, including when this is the invalid-JSON retry."""
    _reserve_call_slot()
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
    _log_usage(response, scenario=scenario)
    return response.choices[0].message.content


def _cache_path(prompt: str) -> Path:
    digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{digest}.json"


def _calls_path() -> Path:
    return CACHE_DIR / "calls.json"


def _usage_path() -> Path:
    return CACHE_DIR / "usage.jsonl"


def _reserve_call_slot() -> int:
    """Increment the on-disk real-call counter, refusing once MAX_LLM_CALLS is hit."""
    calls_path = _calls_path()
    count = 0
    if calls_path.exists():
        count = json.loads(calls_path.read_text()).get("count", 0)
    if count >= MAX_LLM_CALLS:
        raise LLMCallCapError(
            f"MAX_LLM_CALLS cap of {MAX_LLM_CALLS} real Meta Model API calls has "
            f"been reached (see {calls_path}). Raise MAX_LLM_CALLS to allow more."
        )
    count += 1
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    calls_path.write_text(json.dumps({"count": count}))
    return count


def _log_usage(response, *, scenario: str | None) -> None:
    usage = getattr(response, "usage", None)
    if usage is None:
        return
    prompt_tokens = usage.prompt_tokens
    completion_tokens = usage.completion_tokens
    reasoning_tokens = None
    details = getattr(usage, "completion_tokens_details", None)
    if details is not None:
        reasoning_tokens = getattr(details, "reasoning_tokens", None)
    est_cost_usd = (
        prompt_tokens * INPUT_PRICE_PER_TOKEN + completion_tokens * OUTPUT_PRICE_PER_TOKEN
    )
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "scenario": scenario,
        "model": response.model,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "est_cost_usd": round(est_cost_usd, 6),
    }
    if reasoning_tokens is not None:
        record["reasoning_tokens"] = reasoning_tokens
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with _usage_path().open("a") as f:
        f.write(json.dumps(record) + "\n")

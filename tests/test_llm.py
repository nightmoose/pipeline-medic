"""llm.py talks to the Meta Model API through a mocked httpx transport.

No test here needs a real MODEL_API_KEY or makes a real network call.
"""

import json

import httpx
import pytest
from openai import OpenAI

from pipeline_medic import llm

VALID_DIAGNOSIS = {
    "root_cause": "test root cause",
    "evidence": ["evidence 1"],
    "confidence": 0.5,
    "patch": "",
    "action": "suggest",
}


def _chat_response(content: str) -> dict:
    return {
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "created": 0,
        "model": llm.MODEL,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def _patch_openai_with_transport(monkeypatch, handler) -> None:
    def fake_openai(**kwargs):
        return OpenAI(**kwargs, http_client=httpx.Client(transport=httpx.MockTransport(handler)))

    monkeypatch.setattr(llm, "OpenAI", fake_openai)


def test_mock_response_skips_network(monkeypatch, tmp_path):
    monkeypatch.setattr(llm, "CACHE_DIR", tmp_path / "cache")

    diagnosis = llm.diagnose("some context", mock_response=VALID_DIAGNOSIS)

    assert diagnosis.root_cause == "test root cause"
    assert diagnosis.action == "suggest"
    assert not (tmp_path / "cache").exists()


def test_diagnose_calls_api_and_caches(monkeypatch, tmp_path):
    monkeypatch.setattr(llm, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setenv("MODEL_API_KEY", "test-key")
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=_chat_response(json.dumps(VALID_DIAGNOSIS)))

    _patch_openai_with_transport(monkeypatch, handler)

    diagnosis = llm.diagnose("some context")
    assert diagnosis.action == "suggest"
    assert len(calls) == 1
    cached_diagnoses = [
        p for p in (tmp_path / "cache").glob("*.json") if p.name != "calls.json"
    ]
    assert len(cached_diagnoses) == 1

    diagnosis_again = llm.diagnose("some context")
    assert diagnosis_again == diagnosis
    assert len(calls) == 1  # cache hit, no second call


def test_diagnose_retries_once_on_invalid_json(monkeypatch, tmp_path):
    monkeypatch.setattr(llm, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setenv("MODEL_API_KEY", "test-key")
    responses = [
        httpx.Response(200, json=_chat_response("not json")),
        httpx.Response(200, json=_chat_response(json.dumps(VALID_DIAGNOSIS))),
    ]

    def handler(request):
        return responses.pop(0)

    _patch_openai_with_transport(monkeypatch, handler)

    diagnosis = llm.diagnose("some context")

    assert diagnosis.action == "suggest"
    assert responses == []


def test_diagnose_raises_after_second_invalid_json(monkeypatch, tmp_path):
    monkeypatch.setattr(llm, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setenv("MODEL_API_KEY", "test-key")

    def handler(request):
        return httpx.Response(200, json=_chat_response("still not json"))

    _patch_openai_with_transport(monkeypatch, handler)

    with pytest.raises(Exception):
        llm.diagnose("some context")


def _chat_response_with_usage(content: str, *, prompt_tokens: int, completion_tokens: int) -> dict:
    response = _chat_response(content)
    response["usage"] = {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
    }
    return response


def test_real_call_writes_usage_line(monkeypatch, tmp_path):
    monkeypatch.setattr(llm, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setenv("MODEL_API_KEY", "test-key")

    def handler(request):
        return httpx.Response(
            200,
            json=_chat_response_with_usage(
                json.dumps(VALID_DIAGNOSIS), prompt_tokens=1000, completion_tokens=100
            ),
        )

    _patch_openai_with_transport(monkeypatch, handler)

    llm.diagnose("some context", scenario="upstream_rename")

    usage_path = tmp_path / "cache" / "usage.jsonl"
    lines = usage_path.read_text().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["scenario"] == "upstream_rename"
    assert record["model"] == llm.MODEL
    assert record["prompt_tokens"] == 1000
    assert record["completion_tokens"] == 100
    expected_cost = 1000 * llm.INPUT_PRICE_PER_TOKEN + 100 * llm.OUTPUT_PRICE_PER_TOKEN
    assert record["est_cost_usd"] == pytest.approx(expected_cost)
    assert "ts" in record


def test_cache_hit_writes_no_usage_line(monkeypatch, tmp_path):
    monkeypatch.setattr(llm, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setenv("MODEL_API_KEY", "test-key")

    def handler(request):
        return httpx.Response(
            200,
            json=_chat_response_with_usage(
                json.dumps(VALID_DIAGNOSIS), prompt_tokens=10, completion_tokens=10
            ),
        )

    _patch_openai_with_transport(monkeypatch, handler)

    llm.diagnose("some context")
    llm.diagnose("some context")  # cache hit

    usage_path = tmp_path / "cache" / "usage.jsonl"
    assert len(usage_path.read_text().splitlines()) == 1


def test_call_cap_refuses_once_reached(monkeypatch, tmp_path):
    monkeypatch.setattr(llm, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(llm, "MAX_LLM_CALLS", 1)
    monkeypatch.setenv("MODEL_API_KEY", "test-key")
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=_chat_response(json.dumps(VALID_DIAGNOSIS)))

    _patch_openai_with_transport(monkeypatch, handler)

    llm.diagnose("first context")
    assert len(calls) == 1

    with pytest.raises(llm.LLMCallCapError):
        llm.diagnose("second context")  # different prompt: cache miss, should be refused
    assert len(calls) == 1  # refused locally, no second network call

    calls_path = tmp_path / "cache" / "calls.json"
    assert json.loads(calls_path.read_text()) == {"count": 1}


def test_call_cap_counts_invalid_json_retry(monkeypatch, tmp_path):
    monkeypatch.setattr(llm, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(llm, "MAX_LLM_CALLS", 2)
    monkeypatch.setenv("MODEL_API_KEY", "test-key")
    responses = [
        httpx.Response(200, json=_chat_response("not json")),
        httpx.Response(200, json=_chat_response(json.dumps(VALID_DIAGNOSIS))),
    ]

    def handler(request):
        return responses.pop(0)

    _patch_openai_with_transport(monkeypatch, handler)

    llm.diagnose("some context")  # takes 2 real attempts: invalid JSON, then valid

    calls_path = tmp_path / "cache" / "calls.json"
    assert json.loads(calls_path.read_text()) == {"count": 2}

    with pytest.raises(llm.LLMCallCapError):
        llm.diagnose("another context")

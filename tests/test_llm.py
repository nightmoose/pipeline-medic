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
    assert len(list((tmp_path / "cache").glob("*.json"))) == 1

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

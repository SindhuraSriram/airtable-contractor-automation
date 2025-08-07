import pytest
import json
from llm_review import build_prompt, update_llm_fields

def test_build_prompt_with_valid_json():
    sample_json = {
        "personal": {"name": "John", "location": "US"},
        "experience": [{"company": "Google", "title": "Engineer"}],
        "salary": {"rate": 90, "currency": "USD", "availability": 25}
    }
    prompt = build_prompt(sample_json)
    assert "Applicant JSON" in prompt
    assert "Summary:" in prompt

def test_update_llm_fields_parsing(monkeypatch):
    dummy_response = """Summary: Great applicant.
Score: 8
Issues: None
Follow-Ups:
• Are you available next month?
• Have you worked on production systems?
"""

    class DummyTable:
        def update(self, record_id, fields):
            assert "LLM Summary" in fields
            assert "LLM Score" in fields
            assert "LLM Follow-Ups" in fields

    monkeypatch.setattr("llm_review.applicants_tbl", DummyTable())

    try:
        update_llm_fields("rec123", dummy_response)
    except Exception as e:
        pytest.fail(f"update_llm_fields failed: {e}")
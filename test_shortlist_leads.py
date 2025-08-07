import pytest
from shortlist_leads import evaluate_applicant

def test_shortlist_no_compressed_json(monkeypatch):
    dummy_applicant = {"id": "rec123", "fields": {}}

    try:
        evaluate_applicant(dummy_applicant)
    except Exception as e:
        pytest.fail(f"evaluate_applicant failed on missing JSON: {e}")
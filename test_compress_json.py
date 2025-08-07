import pytest
from compress_json import compress_applicant

def test_compress_applicant_with_empty_child_tables(monkeypatch):
    dummy_record = {"id": "rec123"}

    def mock_get_linked_records(tbl, applicant_id):
        return []

    monkeypatch.setattr("compress_json.get_linked_records", mock_get_linked_records)

    # Should not crash or raise exception
    try:
        compress_applicant(dummy_record)
    except Exception as e:
        pytest.fail(f"compress_applicant failed with exception: {e}")
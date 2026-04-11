"""Tests for PII redaction."""

from src.tools.redact import REDACTED, redact_preview


class TestRedactPreview:
    def test_redacts_pii_columns(self) -> None:
        rows = [
            {"Name": "Alice", "Email": "a@b.com", "Phone": "123", "Address": "Main St", "Total_Amount": 100},
            {"Name": "Bob", "Email": "b@c.com", "Phone": "456", "Address": "2nd Ave", "Total_Amount": 200},
        ]
        result = redact_preview(rows, pii_columns=["Name", "Email", "Phone", "Address"])

        for row in result:
            assert row["Name"] == REDACTED
            assert row["Email"] == REDACTED
            assert row["Phone"] == REDACTED
            assert row["Address"] == REDACTED
            # Non-PII should be preserved
            assert isinstance(row["Total_Amount"], (int, float))

    def test_preserves_non_pii(self) -> None:
        rows = [{"City": "Berlin", "Amount": 50}]
        result = redact_preview(rows, pii_columns=["Name", "Email"])
        assert result[0]["City"] == "Berlin"
        assert result[0]["Amount"] == 50

    def test_empty_rows(self) -> None:
        assert redact_preview([], pii_columns=["Name"]) == []

    def test_case_insensitive(self) -> None:
        rows = [{"email": "test@test.com", "Revenue": 100}]
        result = redact_preview(rows, pii_columns=["Email"])
        assert result[0]["email"] == REDACTED
        assert result[0]["Revenue"] == 100

    def test_default_pii_columns(self) -> None:
        """When no pii_columns are specified, uses the default set."""
        rows = [{"Name": "Alice", "Email": "a@b.com", "City": "Berlin"}]
        result = redact_preview(rows)
        assert result[0]["Name"] == REDACTED
        assert result[0]["Email"] == REDACTED
        assert result[0]["City"] == "Berlin"

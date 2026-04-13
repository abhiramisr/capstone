import os

import pytest
from dotenv import load_dotenv

from src.connectors.fabric_connector import FabricConnection


def _fabric_env_ready() -> bool:
    # Allow either a full ODBC connection string or service-principal env mode.
    if os.getenv("FABRIC_CONNECTION_STRING"):
        return True
    required = [
        "FABRIC_SERVER",
        "FABRIC_DATABASE",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
    ]
    return all(os.getenv(k) for k in required)


load_dotenv()

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    not _fabric_env_ready(),
    reason=(
        "Missing Fabric env vars. Set FABRIC_CONNECTION_STRING, or set "
        "FABRIC_SERVER/FABRIC_DATABASE/AZURE_TENANT_ID/AZURE_CLIENT_ID/AZURE_CLIENT_SECRET."
    ),
)
class TestFabricIntegration:
    def test_select_one(self):
        with FabricConnection() as conn:
            row = conn.execute("SELECT 1 AS ok").fetchone()
        assert row is not None
        assert row[0] == 1

    def test_rejects_non_select(self):
        with FabricConnection() as conn:
            with pytest.raises(PermissionError):
                conn.execute("DELETE FROM some_table")

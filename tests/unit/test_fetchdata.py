
"""Unit tests for fetch_products ETL logic."""
from pathlib import Path
import os
import pytest
import requests
from etl.fetch_data import fetch_products

@pytest.mark.parametrize("mock_response,expected_exception", [
    ("notalist", ValueError),
    ([], None),
])
def test_fetch_products_edge_cases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mock_response: str | list,
    expected_exception: type[Exception] | None
):
    """Test fetch_products for edge cases and error handling."""
    class MockResponse:
        """Mock response for requests.get."""
        def __init__(self, data):
            """Initialize with mock data."""
            self._data = data
        def json(self):
            """Return mock JSON data."""
            return self._data
        def raise_for_status(self):
            """Mock raise_for_status does nothing."""
            pass
    def mock_get(*args, **kwargs):
        """Return MockResponse for requests.get."""
        return MockResponse(mock_response)
    monkeypatch.setattr(requests, "get", mock_get)
    out_path = tmp_path / "products.parquet"
    local_file = tmp_path / "mock"
    # Write mock data to file for each case
    if mock_response == "notalist":
        local_file.write_text('"notalist"', encoding="utf-8")  # not a list, triggers ValueError
    else:
        local_file.write_text("[]", encoding="utf-8")  # valid empty list
    if expected_exception:
        with pytest.raises(expected_exception):
            fetch_products(api_url=str(local_file), out_path=str(out_path), output_format="parquet")
    else:
        fetch_products(api_url=str(local_file), out_path=str(out_path), output_format="parquet")
        assert os.path.exists(out_path)

def test_fetch_products_network_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path
):
    """Test fetch_products raises on network error."""
    """Test fetch_products raises on network error."""
    def mock_get(*args, **kwargs):
        """Raise network error for requests.get."""
        raise requests.exceptions.RequestException("Network error")
    monkeypatch.setattr(requests, "get", mock_get)
    out_path = tmp_path / "products.parquet"
    with pytest.raises(requests.exceptions.RequestException):
        fetch_products(api_url="http://mock", out_path=str(out_path), output_format="parquet")

def test_fetch_products_unsupported_format(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path
):
    """Test fetch_products raises on unsupported output format."""
    """Test fetch_products raises on unsupported output format."""
    class MockResponse:
        """Mock response for unsupported format."""
        def json(self):
            """Return empty list for unsupported format."""
            return []
        def raise_for_status(self):
            """Mock raise_for_status does nothing."""
            pass
    monkeypatch.setattr(requests, "get", lambda *a, **k: MockResponse())
    out_path = tmp_path / "products.unsupported"
    with pytest.raises(ValueError):
        fetch_products(api_url="http://mock", out_path=str(out_path), output_format="unsupported")
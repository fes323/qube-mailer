import pytest


@pytest.fixture(autouse=True)
def no_preset_routes(monkeypatch, request):
    """Older tests expect an empty route table; tests/test_presets.py turns presets back on."""
    if request.module.__name__.endswith("test_presets"):
        return
    monkeypatch.setattr("app.presets.PRESETS", {})
    monkeypatch.setattr("app.presets.AUTOREPLY_PRESETS", {})

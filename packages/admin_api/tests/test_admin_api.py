import admin_api


def test_admin_api_main_calls_uvicorn(monkeypatch) -> None:
    called = {}

    def fake_run(*args, **kwargs):
        called["args"] = args
        called["kwargs"] = kwargs

    monkeypatch.setattr(admin_api.uvicorn, "run", fake_run)
    admin_api.main()

    assert called["args"][0] == "admin_api.app:app"
    assert called["kwargs"]["host"] == "0.0.0.0"
    assert called["kwargs"]["port"] == 8000

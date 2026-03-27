import admin_api


def test_admin_api_main_prints_started_message(capsys) -> None:
    admin_api.main()
    captured = capsys.readouterr()
    assert "admin_api service started" in captured.out

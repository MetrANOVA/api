from metranova.main import main, run_admin_api, run_pipeline


def test_run_admin_api_prints_started_message(capsys) -> None:
    run_admin_api()
    captured = capsys.readouterr()
    assert "admin_api service started" in captured.out


def test_run_pipeline_prints_started_message(capsys) -> None:
    run_pipeline()
    captured = capsys.readouterr()
    assert "pipeline service started" in captured.out


def test_main_prints_core_message(capsys) -> None:
    main()
    captured = capsys.readouterr()
    assert "metranova core package" in captured.out

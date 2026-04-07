import pipeline


def test_pipeline_main_prints_started_message(capsys) -> None:
    pipeline.main()
    captured = capsys.readouterr()
    assert "pipeline service started" in captured.out

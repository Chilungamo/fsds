from radar import config


def test_database_url_precedence(monkeypatch):
    monkeypatch.delenv("RADAR_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert config.database_url() == config.DEFAULT_DATABASE_URL
    monkeypatch.setenv("DATABASE_URL", "postgresql://fly/db")
    assert config.database_url() == "postgresql://fly/db"
    monkeypatch.setenv("RADAR_DATABASE_URL", "postgresql://explicit/db")
    assert config.database_url() == "postgresql://explicit/db"

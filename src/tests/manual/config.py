from src.ORMrepo.db_settings import get_config, set_config, update_config, Config, ConfigUpdate, DBSettings
print(get_config())
set_config(Config(db=DBSettings(dsn="sqlite+aiosqlite:///test.db")))
print(get_config())
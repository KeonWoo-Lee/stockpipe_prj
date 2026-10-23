from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from stockpipe.config import Settings

def get_engine(settings: Settings | None = None) -> Engine:
	if settings is None:
		settings = Settings()
	return create_engine(settings.database_url, pool_pre_ping=True)

def get_session_factory(engine: Engine) -> sessionmaker:
	return sessionmaker(bind=engine)

def init_db(engine: Engine, sql_path: str = "scripts/init_db.sql") -> None:
	with open(sql_path) as f:
		sql = f.read()
	with engine.begin() as conn:
		conn.execute(text(sql))

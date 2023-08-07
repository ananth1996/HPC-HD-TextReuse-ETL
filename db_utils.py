from sqlalchemy import create_engine
import toml
from pathlib import Path
project_root = Path(__file__).parent.resolve()

with open(project_root/"database.toml") as fp:
    db_options = toml.load(fp)


def get_sqlalchemy_connection():
    opts = db_options["mariadb"]
    engine_string = f"mysql+pymysql://{opts['user']}:{opts['password']}@{opts['host']}/{opts['database']}?charset=utf8mb4"
    engine = create_engine(engine_string, future=True)
    conn = engine.connect()
    return conn
from sqlalchemy import create_engine
from pathlib import Path
from sqlalchemy.pool import NullPool
project_root = Path(__file__).parent.parent.resolve()
import os 

def get_sqlalchemy_connect(version):
    engine = get_sqlalchemy_engine(version)
    conn = engine.connect()
    return conn

def get_sqlalchemy_engine():
    engine_string = f"mysql+pymysql://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}/{os.environ['DB_DATABASE']}?charset=utf8mb4"
    engine = create_engine(engine_string, future=True, poolclass=NullPool, pool_pre_ping=True)
    return engine
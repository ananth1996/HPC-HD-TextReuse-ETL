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
    engine_string = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_DATABASE')}?charset=utf8mb4"
    engine = create_engine(engine_string, future=True, poolclass=NullPool, pool_pre_ping=True)
    return engine
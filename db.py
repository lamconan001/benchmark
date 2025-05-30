# Database drivers module
from sqlalchemy import create_engine, text

class DBExecutor:
    def __init__(self, engine_url):
        self.engine_url = engine_url

    def get_pk_values(self, table, pk_col):
        engine = create_engine(self.engine_url)
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT {pk_col} FROM {table}"))
            return [row[0] for row in result]

    def exec_query(self, sql, params=None):
        engine = create_engine(self.engine_url)
        with engine.begin() as conn:
            if params:
                conn.execute(text(sql), params)
            else:
                conn.execute(text(sql))

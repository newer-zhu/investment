import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import MetaData, create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


DEFAULT_DB_URL = (
    "mysql+pymysql://root:20001030@localhost:3306/finance?charset=utf8mb4"
)


def create_db_engine(db_url: Optional[str] = None, pool_size: int = 5, max_overflow: int = 10) -> Engine:
    """
    Create a SQLAlchemy Engine.
    db_url priority: provided arg > env DB_URL > DEFAULT_DB_URL
    """
    url = db_url or os.getenv("DB_URL") or DEFAULT_DB_URL
    return create_engine(
        url,
        pool_recycle=1800,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        future=True,
    )


class DbUtils:
    """
    Lightweight DB helper for common tasks on MySQL via SQLAlchemy.
    - Query to DataFrame
    - Bulk insert DataFrame
    - Upsert DataFrame with ON DUPLICATE KEY UPDATE (requires unique/primary keys)
    - Execute raw SQL
    """

    def __init__(self, engine: Optional[Engine] = None, db_url: Optional[str] = None) -> None:
        self.engine: Engine = engine or create_db_engine(db_url=db_url)
        self._inspector = inspect(self.engine)
        self._metadata = MetaData()

    # ---------- Basics ----------
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(sql), params or {})

    def query_df(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        with self.engine.begin() as conn:
            return pd.read_sql(text(sql), conn, params=params)

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        return self._inspector.has_table(table_name, schema=schema)

    # ---------- DataFrame I/O ----------
    def to_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
        dtype: Optional[Dict[str, Any]] = None,
        chunksize: int = 1000,
        method: str = "multi",
    ) -> None:
        if df is None or df.empty:
            return
        with self.engine.begin() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists=if_exists,
                index=index,
                dtype=dtype,
                chunksize=chunksize,
                method=method,
            )

    # ---------- Upsert ----------
    def upsert_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
        chunksize: int = 1000,
    ) -> None:
        """
        Perform INSERT ... ON DUPLICATE KEY UPDATE for MySQL.
        Requires a unique index or primary key on key_columns.
        """
        if df is None or df.empty:
            return
        if not key_columns:
            raise ValueError("key_columns is required for upsert.")

        cols: List[str] = list(df.columns)
        if update_columns is None:
            update_columns = [c for c in cols if c not in key_columns]

        placeholders = ", ".join([f":{c}" for c in cols])
        columns_joined = ", ".join([f"`{c}`" for c in cols])
        update_clause = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in update_columns])

        sql = f"INSERT INTO `{table_name}` ({columns_joined}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause};"

        records = df.where(pd.notna(df), None).to_dict(orient="records")

        with self.engine.begin() as conn:
            for i in range(0, len(records), chunksize):
                batch = records[i : i + chunksize]
                conn.execute(text(sql), batch)

    # ---------- Schema helpers ----------
    def ensure_index(
        self,
        table_name: str,
        index_name: str,
        columns: Sequence[str],
        unique: bool = False,
        schema: Optional[str] = None,
    ) -> None:
        """
        Create index if not exists.
        """
        cols = ", ".join([f"`{c}`" for c in columns])
        unique_sql = "UNIQUE " if unique else ""
        schema_prefix = f"`{schema}`." if schema else ""
        sql = f"CREATE {unique_sql}INDEX IF NOT EXISTS `{index_name}` ON {schema_prefix}`{table_name}` ({cols});"
        # MySQL before 8.0 does not support IF NOT EXISTS for indexes; fallback logic:
        try:
            self.execute(sql)
        except SQLAlchemyError:
            # Check indexes and create if missing
            existing = {ix["name"] for ix in self._inspector.get_indexes(table_name, schema=schema)}
            if index_name not in existing:
                fallback_sql = f"CREATE {unique_sql}INDEX `{index_name}` ON {schema_prefix}`{table_name}` ({cols});"
                self.execute(fallback_sql)

    def ensure_primary_key(self, table_name: str, columns: Sequence[str], schema: Optional[str] = None) -> None:
        """
        Add primary key if table has none. MySQL requires column(s) to be NOT NULL.
        """
        pk = self._inspector.get_pk_constraint(table_name, schema=schema)
        if pk and pk.get("constrained_columns"):
            return
        cols = ", ".join([f"`{c}`" for c in columns])
        schema_prefix = f"`{schema}`." if schema else ""
        self.execute(f"ALTER TABLE {schema_prefix}`{table_name}` ADD PRIMARY KEY ({cols});")



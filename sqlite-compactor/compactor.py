import datetime
import logging
import os
import sqlite3
import duckdb
from typing import List
from query_builder import QueryBuilder, compact_table

from ulid import ULID

logger = logging.getLogger(__name__)


class NotEnoughRowsToCompact(Exception):
    def __init__(self, row_count: int):
        super().__init__(f"Table has {row_count} rows, not enough to compact.")
        self.row_count = row_count


class Compactor:
    def __init__(
        self,
        sqlite_database_path: str,
        tables: List[str],
        output_directory: str,
        min_rows_to_compact: int,
    ):
        self.sqlite_database_path = sqlite_database_path
        self.tables = tables
        self.output_directory = output_directory
        self.min_rows_to_compact = min_rows_to_compact
        logger.debug(
            f"Created compactor {self.sqlite_database_path=}, {self.tables=}, {self.output_directory=} {self.min_rows_to_compact=}"
        )

    def compact(self) -> None:
        initial_database_size = self._get_database_size_mib()

        run_id = str(ULID())
        tables_to_compact = []
        for table_name in self.tables:
            table_size = self._get_table_size(table_name)
            if table_size < self.min_rows_to_compact:
                logger.info(
                    f"Skipping table {table_name!r}, not enough rows to compact ({table_size:,})"
                )
            else:
                logger.debug(
                    f"Table {table_name!r} has {table_size:,} rows, will compact."
                )
                tables_to_compact.append(table_name)

        if not tables_to_compact:
            logger.info("No tables to compact, exiting.")
            return

        sql = self._create_sql_query(run_id, tables_to_compact)
        logger.debug("Generated SQL query: \n" + sql)

        logger.info(f"Starting compaction with {run_id=}")
        with duckdb.connect() as con:
            con.sql(sql)

        self._vacuum_database()

        final_database_size = self._get_database_size_mib()
        logger.info(
            f"Vacuumed the database, {initial_database_size=:,.3f} MiB, {final_database_size=:,.3f} MiB"
        )

    def _create_sql_query(self, run_id: str, tables_to_compact: List[str]) -> str:
        qb = QueryBuilder()
        qb = qb.load_sqlite_table(self.sqlite_database_path)
        qb = qb.create_compaction_table()

        for table_name in tables_to_compact:
            file_name = f"{table_name}-{run_id}.parquet"
            file_path = os.path.join(self.output_directory, file_name)
            qb = qb.transaction(compact_table(run_id, table_name, file_path, "rowid"))

        return qb.build()

    def _get_database_size_mib(self):
        return os.stat(self.sqlite_database_path).st_size / 1024 / 1024

    def _get_table_size(self, table_name: str) -> int:
        with sqlite3.connect(self.sqlite_database_path) as con:
            return con.execute(f"SELECT COUNT(1) FROM {table_name}").fetchone()[0]

    def _vacuum_database(self) -> None:
        with sqlite3.connect(self.sqlite_database_path) as con:
            con.execute("VACUUM")


def _create_compaction_table(sqlite_con_factory: sqlite3.Connection):
    with sqlite_con_factory as con:
        statement = "CREATE TABLE IF NOT EXISTS compactions (id TEXT, timestamp_ms INTEGER, file_name TEXT, table_name TEXT)"
        con.execute(statement)


def _get_unix_timestamp_ms(t: datetime.datetime) -> int:
    ts = (t - datetime.datetime(1970, 1, 1)) / datetime.timedelta(milliseconds=1)
    return int(ts)


def _attempt_to_delete(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass

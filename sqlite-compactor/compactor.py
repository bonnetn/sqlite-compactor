import datetime
import logging
import os
import sqlite3
from pathlib import Path
from typing import List

from ulid import ULID

import pandas as pd

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

        sqlite3_connection_factory = sqlite3.connect(self.sqlite_database_path)
        _create_compaction_table(sqlite3_connection_factory)

        start_time = datetime.datetime.now()
        run_id = str(ULID())
        logger.info(f"Starting compaction with {run_id=}")

        for table_name in self.tables:
            logger.debug(f"Compacting table {table_name!r}")
            try:
                rows_compacted = self.compact_table(
                    start_time, run_id, sqlite3_connection_factory, table_name
                )
                logger.info(
                    f"Compacted {rows_compacted:,} rows from table {table_name!r}"
                )
            except NotEnoughRowsToCompact as e:
                logger.info(
                    f"Skipping table {table_name!r} because it has too few rows ({e.row_count:,}) to compact."
                )

        self._vacuum_database()

        final_database_size = self._get_database_size_mib()
        logger.info(
            f"Vacuumed the database, {initial_database_size=:.3} MiB, {final_database_size=:.3} MiB"
        )

    def compact_table(
        self,
        start_time: datetime.datetime,
        run_id: str,
        sqlite_con_factory: sqlite3.Connection,
        table_name: str,
    ) -> int:
        directory_path = os.path.join(self.output_directory, table_name)

        temporary_file_name = f"temp-{run_id}.parquet"
        temporary_file_path = os.path.join(directory_path, temporary_file_name)

        committed_file_name = f"committed-{run_id}.parquet"
        committed_file_path = os.path.join(directory_path, committed_file_name)

        try:
            with sqlite_con_factory as con:
                query = f"DELETE FROM {table_name} RETURNING *"
                df = pd.read_sql_query(query, con)
                if len(df) < self.min_rows_to_compact:
                    raise NotEnoughRowsToCompact(len(df))
                logger.debug(f"Loaded {len(df):,} rows from {table_name!r}")

                statement = "INSERT INTO compactions (id, timestamp_ms, file_name, table_name) VALUES (?, ?, ?, ?)"
                con.execute(
                    statement,
                    (
                        run_id,
                        _get_unix_timestamp_ms(start_time),
                        committed_file_name,
                        table_name,
                    ),
                )
                logger.debug("Wrote compaction entry in the database.")

                Path(directory_path).mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created directory {directory_path!r}")

                df.to_parquet(temporary_file_path, engine="pyarrow", compression="gzip")
                logger.debug(f"Wrote {temporary_file_path!r}")

                os.rename(temporary_file_path, committed_file_path)
                logger.debug(f"Moved {temporary_file_name} to {committed_file_name}.")

                return len(df)

        except Exception:
            logger.debug(f"Failed to commit, cleaning up {temporary_file_path}")
            _attempt_to_delete(temporary_file_path)
            raise

    def _get_database_size_mib(self):
        return os.stat(self.sqlite_database_path).st_size / 1024 / 1024

    def _vacuum_database(self):
        sqlite3.connect(self.sqlite_database_path).execute("VACUUM")


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

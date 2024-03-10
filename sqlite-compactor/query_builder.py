from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class QueryBuilder:
    _queries: Tuple[str, ...] = tuple()

    def load_sqlite_table(self, table_path: str) -> QueryBuilder:
        return self._write(
            "LOAD 'sqlite';",
            f"ATTACH '{table_path}' AS db (TYPE sqlite);",
            "USE db;",
        )

    def transaction(self, qb: QueryBuilder) -> QueryBuilder:
        return self._write("BEGIN TRANSACTION;")._write(*qb._queries)._write("COMMIT;")

    def create_compaction_table(self) -> QueryBuilder:
        return self._write(
            """
            CREATE TABLE IF NOT EXISTS compaction_results (
                id TEXT,
                timestamp_ms INTEGER,
                file_name TEXT,
                table_name TEXT
            );"""
        )

    def copy_table_to_parquet(
        self, table_name: str, file_name: str, id_field: str
    ) -> QueryBuilder:
        return self._write(
            f"COPY (SELECT {id_field} AS _compacter_row_id, * FROM {table_name}) TO '{file_name}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100_000);"
        )

    def delete_rows_from_table(
        self, table_name: str, file_name: str, id_field: str
    ) -> QueryBuilder:
        return self._write(
            f"DELETE FROM {table_name} tbl USING '{file_name}' backup WHERE tbl.{id_field} = backup._compacter_row_id;"
        )

    def insert_compaction_table(
        self, run_id: str, table_name: str, file_name: str
    ) -> QueryBuilder:
        return self._write(
            f"INSERT INTO compaction_results (id, timestamp_ms, file_name, table_name) "
            f"VALUES ('{run_id}', epoch_ms(get_current_timestamp()), '{file_name}', '{table_name}');"
        )

    def _write(self, *query: str) -> QueryBuilder:
        sanitized_queries = tuple(_clean_query(q) for q in query)
        return QueryBuilder(self._queries + sanitized_queries)

    def build(self) -> str:
        return "\n".join(self._queries)


def _clean_query(q: str) -> str:
    return re.sub(r"^[ ]+", "", q, flags=re.MULTILINE)


def compact_table(
    run_id: str, table_name: str, file_name: str, id_field: str
) -> QueryBuilder:
    qb = QueryBuilder()
    qb = qb.insert_compaction_table(run_id, table_name, file_name)
    qb = qb.copy_table_to_parquet(table_name, file_name, id_field)
    qb = qb.delete_rows_from_table(table_name, file_name, id_field)
    return qb

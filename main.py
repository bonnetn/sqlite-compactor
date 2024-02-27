import argparse
import logging
import pathlib

from compactor import Compactor


def _positive_int(s: any):
    v = int(s)
    if v < 0:
        raise argparse.ArgumentTypeError("Must be a positive integer.")
    return v


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser(description="Compact specified tables from a SQLite database into parquet files.")
    parser.add_argument("sqlite_database_path", type=pathlib.Path, help="Path to the SQLite database file.")
    parser.add_argument("output_directory", type=pathlib.Path,
                        help="Path to the output directory for compacted tables.")
    parser.add_argument("tables", nargs='+', type=str, help="List of table names to compact.")
    parser.add_argument("--min_rows_to_compact", type=_positive_int, default=100_000,
                        help="Minimum number of rows to trigger compaction.")

    args = parser.parse_args()
    Compactor(
        sqlite_database_path=args.sqlite_database_path,
        tables=args.tables,
        output_directory=args.output_directory,
        min_rows_to_compact=args.min_rows_to_compact,
    ).compact()

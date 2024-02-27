# SQLite compactor

Just a small script that dumps the contents of a SQLite database to parquet files

It deletes all rows from the specified tables, backs them up in a parquet file, and then vacuums the database.

Useful if you have an app that writes events to a SQLite database, and you want to keep the database small.

**Note**: This script also creates a `compactions` table with the file names of the parquet files created. 
It is guaranteed that the parquet files listed in the `compactions` table contain all the rows that were deleted from the SQLite database (exactly once).

## Usage

```
usage: main.py [-h] [--min_rows_to_compact MIN_ROWS_TO_COMPACT]
               sqlite_database_path output_directory tables [tables ...]

Compact specified tables from a SQLite database into parquet files.

positional arguments:
  sqlite_database_path  Path to the SQLite database file.
  output_directory      Path to the output directory for compacted tables.
  tables                List of table names to compact.

options:
  -h, --help            show this help message and exit
  --min_rows_to_compact MIN_ROWS_TO_COMPACT
                        Minimum number of rows to trigger compaction.
```

### Example

```sh
$ docker run \
  -it \
  --rm \
  -v "$(pwd)"/my_sqlite_database.db:/database.db \
  -v "$(pwd)"/directory_for_dumping_data:/dumps/ \
  ghcr.io/bonnetn/sqlite-compactor \
  /database.db \
  /dumps \
  events member_presence
```
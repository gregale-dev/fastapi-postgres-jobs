"""Apply schema.sql. Gregale runs this as the release command, before a new
version takes traffic; a failure leaves the previous version serving."""

import os
import pathlib

import psycopg


def main() -> None:
    url = os.environ.get("MIGRATION_DATABASE_URL") or os.environ["DATABASE_URL"]
    schema = pathlib.Path(__file__).with_name("schema.sql").read_text()
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(schema)
    print("schema applied")


if __name__ == "__main__":
    main()

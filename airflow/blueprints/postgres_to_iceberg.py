"""Blueprint for ingesting Postgres tables to Iceberg tables in S3."""

from __future__ import annotations

import os
from typing import Any

from airflow.sdk import TaskGroup, task
from blueprint import Blueprint
from pydantic import BaseModel, Field



def _downcast_ns_timestamps(table: "pa.Table") -> "pa.Table":
    """Cast any nanosecond timestamp column to microseconds.

    Iceberg supports microsecond precision at most, and pandas produces
    timestamp[ns] by default, so writing an untouched frame fails with
    UnsupportedPyArrowTypeException. Values are truncated, not rounded.
    """
    import pyarrow as pa

    fields = []
    changed = False
    for field in table.schema:
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            fields.append(field.with_type(pa.timestamp("us", tz=field.type.tz)))
            changed = True
        else:
            fields.append(field)

    if not changed:
        return table
    return table.cast(pa.schema(fields))


class PostgresToIcebergConfig(BaseModel):
    """Configuration for Postgres to Iceberg ingestion."""

    table_name: str = Field(..., description="Name of the Postgres table to ingest")
    schema_name: str = Field(default="public", description="Postgres schema name")
    iceberg_namespace: str = Field(
        default="default", description="Iceberg namespace/database"
    )
    iceberg_table_name: str | None = Field(
        default=None, description="Iceberg table name (defaults to source table name)"
    )
    s3_bucket: str = Field(
        default="iceberg-warehouse", description="S3 bucket for Iceberg warehouse"
    )
    s3_prefix: str = Field(
        default="warehouse", description="S3 prefix/path for Iceberg warehouse"
    )
    mode: str = Field(
        default="overwrite",
        description="Write mode: 'overwrite' or 'append'",
    )
    batch_size: int = Field(
        default=10000, description="Number of rows to fetch per batch"
    )
    postgres_conn_id: str = Field(
        default="postgres_banking", description="Airflow connection ID for source Postgres"
    )


class PostgresToIcebergBlueprint(Blueprint[PostgresToIcebergConfig]):
    """Blueprint for extracting Postgres tables to Iceberg format in S3."""

    def render(self, config: PostgresToIcebergConfig) -> TaskGroup:
        """Render the task group for Postgres to Iceberg ingestion."""

        iceberg_table_name = config.iceberg_table_name or config.table_name

        with TaskGroup(group_id=f"ingest_{config.table_name}") as task_group:

            @task(task_id="extract_postgres_schema")
            def extract_postgres_schema(**context) -> dict[str, Any]:
                """Extract table schema from Postgres."""
                import psycopg2
                from airflow.sdk import BaseHook

                conn = BaseHook.get_connection(config.postgres_conn_id)
                pg_conn = psycopg2.connect(
                    host=conn.host,
                    port=conn.port,
                    database=conn.schema,
                    user=conn.login,
                    password=conn.password,
                )

                cursor = pg_conn.cursor()
                cursor.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """,
                    (config.schema_name, config.table_name),
                )

                columns = []
                for row in cursor.fetchall():
                    columns.append(
                        {
                            "name": row[0],
                            "type": row[1],
                            "nullable": row[2] == "YES",
                        }
                    )

                cursor.close()
                pg_conn.close()

                return {"columns": columns, "table_name": config.table_name}

            @task(task_id="extract_postgres_data")
            def extract_postgres_data(schema_info: dict[str, Any], **context) -> str:
                """Extract data from Postgres and save as Parquet."""
                import tempfile

                import pandas as pd
                import psycopg2
                from airflow.sdk import BaseHook

                conn = BaseHook.get_connection(config.postgres_conn_id)
                pg_conn = psycopg2.connect(
                    host=conn.host,
                    port=conn.port,
                    database=conn.schema,
                    user=conn.login,
                    password=conn.password,
                )

                # Read data in batches
                query = f"SELECT * FROM {config.schema_name}.{config.table_name}"
                chunks = []

                for chunk in pd.read_sql(
                    query, pg_conn, chunksize=config.batch_size
                ):
                    chunks.append(chunk)

                pg_conn.close()

                # Combine all chunks
                if chunks:
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    df = pd.DataFrame()

                # Save to temporary parquet file.
                # pandas renders datetimes as timestamp[ns], which Iceberg does
                # not support (it tops out at microsecond precision), so every
                # nanosecond column is downcast to us before the file is written.
                import pyarrow as pa
                import pyarrow.parquet as pq

                arrow_table = pa.Table.from_pandas(df, preserve_index=False)
                arrow_table = _downcast_ns_timestamps(arrow_table)

                temp_file = tempfile.NamedTemporaryFile(
                    delete=False, suffix=".parquet"
                )
                pq.write_table(arrow_table, temp_file.name)

                print(f"Extracted {len(df)} rows from {config.table_name}")
                return temp_file.name

            @task(task_id="create_iceberg_table")
            def create_iceberg_table(
                schema_info: dict[str, Any], **context
            ) -> dict[str, str]:
                """Create or update Iceberg table."""
                import pyarrow as pa
                from pyiceberg.catalog import load_catalog
                from pyiceberg.schema import Schema
                from pyiceberg.types import (
                    BooleanType,
                    DateType,
                    DoubleType,
                    FloatType,
                    IntegerType,
                    LongType,
                    StringType,
                    TimestampType,
                )

                # Map Postgres types to Iceberg types
                type_mapping = {
                    "integer": IntegerType(),
                    "bigint": LongType(),
                    "smallint": IntegerType(),
                    "real": FloatType(),
                    "double precision": DoubleType(),
                    "numeric": DoubleType(),
                    "decimal": DoubleType(),
                    "text": StringType(),
                    "character varying": StringType(),
                    "varchar": StringType(),
                    "char": StringType(),
                    "character": StringType(),
                    "boolean": BooleanType(),
                    "date": DateType(),
                    "timestamp without time zone": TimestampType(),
                    "timestamp with time zone": TimestampType(),
                }

                # Build Iceberg schema
                from pyiceberg.schema import NestedField

                fields = []
                for idx, col in enumerate(schema_info["columns"], start=1):
                    pg_type = col["type"].lower()
                    iceberg_type = type_mapping.get(pg_type, StringType())
                    fields.append(
                        NestedField(
                            field_id=idx,
                            name=col["name"],
                            field_type=iceberg_type,
                            required=not col["nullable"],
                        )
                    )

                iceberg_schema = Schema(*fields)

                # Persistent REST catalog (iceberg-rest service, backed by Postgres)
                # so table pointers survive restarts and are shared across containers.
                catalog_config = {
                    "type": "rest",
                    "uri": os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"),
                    "warehouse": f"s3://{config.s3_bucket}/{config.s3_prefix}",
                    "s3.endpoint": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
                    "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                    "s3.secret-access-key": os.getenv(
                        "AWS_SECRET_ACCESS_KEY", "minioadmin"
                    ),
                }

                catalog = load_catalog("default", **catalog_config)

                # Create namespace if it doesn't exist
                try:
                    catalog.create_namespace(config.iceberg_namespace)
                except Exception:
                    pass  # Namespace might already exist

                table_identifier = f"{config.iceberg_namespace}.{iceberg_table_name}"

                # Create or replace table
                try:
                    if config.mode == "overwrite":
                        try:
                            catalog.drop_table(table_identifier)
                        except Exception:
                            pass

                        table = catalog.create_table(
                            identifier=table_identifier,
                            schema=iceberg_schema,
                        )
                        print(f"Created new Iceberg table: {table_identifier}")
                    else:
                        table = catalog.load_table(table_identifier)
                        print(f"Loaded existing Iceberg table: {table_identifier}")
                except Exception as e:
                    # Table doesn't exist, create it
                    table = catalog.create_table(
                        identifier=table_identifier,
                        schema=iceberg_schema,
                    )
                    print(f"Created new Iceberg table: {table_identifier}")

                return {
                    "table_identifier": table_identifier,
                    "warehouse": catalog_config["warehouse"],
                }

            @task(task_id="load_to_iceberg")
            def load_to_iceberg(
                parquet_file: str, iceberg_info: dict[str, str], **context
            ) -> dict[str, Any]:
                """Load Parquet data into Iceberg table."""
                import os

                import pyarrow.parquet as pq
                from pyiceberg.catalog import load_catalog

                catalog_config = {
                    "type": "rest",
                    "uri": os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"),
                    "warehouse": iceberg_info["warehouse"],
                    "s3.endpoint": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
                    "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                    "s3.secret-access-key": os.getenv(
                        "AWS_SECRET_ACCESS_KEY", "minioadmin"
                    ),
                }

                catalog = load_catalog("default", **catalog_config)
                table = catalog.load_table(iceberg_info["table_identifier"])

                # Read parquet file. The extract step already downcasts ns
                # timestamps, but guard here too so a file written by an older
                # run cannot fail the Iceberg write.
                arrow_table = _downcast_ns_timestamps(pq.read_table(parquet_file))

                # Align with the target Iceberg schema. pandas widens ints to
                # int64 and marks every column nullable, which Iceberg rejects
                # for int32 or NOT NULL columns, so cast to the table's own
                # Arrow schema before writing.
                arrow_table = arrow_table.cast(table.schema().as_arrow())

                # Append data to Iceberg table
                if config.mode == "overwrite":
                    table.overwrite(arrow_table)
                else:
                    table.append(arrow_table)

                # Clean up temporary file
                os.unlink(parquet_file)

                row_count = len(arrow_table)
                print(
                    f"Loaded {row_count} rows into Iceberg table {iceberg_info['table_identifier']}"
                )

                return {
                    "table": iceberg_info["table_identifier"],
                    "rows_loaded": row_count,
                    "mode": config.mode,
                }

            # Define task dependencies
            schema_info = extract_postgres_schema()
            parquet_file = extract_postgres_data(schema_info)
            iceberg_info = create_iceberg_table(schema_info)
            result = load_to_iceberg(parquet_file, iceberg_info)

        return task_group

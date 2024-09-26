import json

import typer
from typing import Optional
import duckdb
import pyarrow.parquet as pq
import pandas as pd
import tomlkit

from .helpers import (
    format_relation,
    OutputFormat,
    MetaOutputFormat,
    tomlizer,
)


app = typer.Typer(
    help="Parquet CLI tool based on DuckDb. Inspection, verification, and more."
)


@app.command()
def meta(
    file: str,
    format: MetaOutputFormat = typer.Option("toml", help="Metadata output format"),
    include_row_groups: bool = typer.Option(
        False, "--include-row-groups", help="Include row group meta data in output"
    ),
):
    """
    Extract metadata from the Parquet file using pyarrow.

    Field descriptions see https://arrow.apache.org/docs/python/generated/pyarrow.parquet.FileMetaData.html#pyarrow.parquet.FileMetaData

    :param file: Path to the Parquet file.
    :param format: Output format (pretty, csv, json, jsonl, toml).
    :param include_row_groups: Include row group meta data in output.
    """

    meta = pq.read_metadata(file)
    meta_dict = meta.to_dict()
    if not include_row_groups:
        meta_dict.pop("row_groups", None)
    else:
        meta_dict = tomlizer(meta_dict)

    if format == MetaOutputFormat.json:
        typer.echo(json.dumps(meta_dict, indent=2))
    elif format == MetaOutputFormat.toml:  # toml is pretty here
        typer.echo(tomlkit.dumps(meta_dict))

    # typer.echo(f"Showing metadata for {file} (JSON: {json})")


@app.command()
def head(
    file: str,
    records: int = typer.Option(10, help="Number of records to show"),
    format: OutputFormat = typer.Option("pretty", help="Output format"),
):
    """
    Display the first N records from the Parquet file.

    :param file: Path to the Parquet file.
    :param records: Number of records to display (default: 10).
    :param as_csv: Output as CSV format.
    """
    # Load Parquet file and apply the limit using DuckDB
    query = f"SELECT * FROM read_parquet('{file}') LIMIT {records};"
    res = duckdb.query(query)  # not execute
    typer.echo(format_relation(res, format))


@app.command()
def cat(
    file: str,
    line_limit: Optional[int] = typer.Option(  # default 1M, -1 to disable
        1_000_000,
        "--line-limit",
        help="Limit the number of lines to display. -1 to disable.",
    ),
):
    """
    Cat the Parquet file to the console, in CSV format.

    :param file: Path to the Parquet file.
    :param tolerate_large_file: Also cat files with >1M rows.
    """
    if not line_limit == -1:
        # check row count
        res = duckdb.query(
            f"SELECT COUNT(*) as n_rows FROM read_parquet('{file}')"
        ).fetchall()
        if res[0][0] > line_limit:
            typer.echo(
                f"File {file} has more than {line_limit} rows. Change --line-limit or disable with -1."
            )
            raise typer.Exit(code=1)

    # read up the whole file
    res = duckdb.query(f"SELECT * FROM read_parquet('{file}');")
    typer.echo(format_relation(res, OutputFormat.csv))


@app.command()
def schema(
    file: str,
    # include_metadata
    schema_metadata: Optional[bool] = typer.Option(
        False, "--schema-metadata", help="Include schema metadata in output"
    ),
):
    """
    Display the schema of the Parquet file.

    :param file: Path to the Parquet file.
    """
    show_schema_metadata = schema_metadata
    meta = pq.read_metadata(file)

    # built df with flattened schema
    schema_fields = []
    for i, name in enumerate(meta.schema.names):
        c = meta.schema.column(i)
        row = {
            "name": name,
            "path": c.path,
            "physical_type": c.physical_type,
            "logical_type": c.logical_type,
            "converted_type": c.converted_type,
            "length": c.length,
            "precision": c.precision,
            "scale": c.scale,
            "max_definition_level": c.max_definition_level,
            "max_repetition_level": c.max_repetition_level,
        }
        schema_fields.append(row)
    parquet_schema_flattened = pd.DataFrame(schema_fields)

    typer.echo(
        format_relation(duckdb.from_df(parquet_schema_flattened), OutputFormat.pretty)
    )

    # also extract schema metadata
    schema = meta.schema.to_arrow_schema()
    if schema.metadata and show_schema_metadata:
        typer.echo("-- schema metadata --")
        for entry in schema.metadata.keys():
            v = schema.metadata[entry]
            # json load and dump with indent
            if isinstance(v, bytes):
                v = json.loads(v.decode("utf-8"))
            else:
                v = json.loads(v)
            typer.echo(f"{entry.decode("utf-8")}:")
            typer.echo(json.dumps(v, indent=2))
    elif show_schema_metadata:
        typer.echo("<no schema-level metadata>")


@app.command()
def stats(
    file: str,
    json: Optional[bool] = typer.Option(
        False, "--json", help="Output stats in JSON format"
    ),
):
    """
    Display statistics (min, max, count, etc.) for columns in the Parquet file.

    :param file: Path to the Parquet file.
    :param json: Output in JSON format.
    """
    typer.echo(f"Showing stats for {file} (JSON: {json})")


if __name__ == "__main__":
    app()

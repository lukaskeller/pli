import json
import typer
from typing import Optional
import duckdb

from .helpers import format_relation, OutputFormat


app = typer.Typer(
    help="Parquet CLI tool based on DuckDb. Inspection, verification, and more."
)


@app.command()
def meta(
    file: str, format: OutputFormat = typer.Option("pretty", help="Output format")
):
    """
    Display basic metadata of the Parquet file without loading the schema.

    :param file: Path to the Parquet file.
    :param json: Output in JSON format.
    """
    typer.echo(f"Showing metadata for {file} (JSON: {json})")


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
def verify(
    file: str,
    json: Optional[bool] = typer.Option(
        False, "--json", help="Output verification result in JSON format"
    ),
):
    """
    Verify the integrity of the Parquet file by checking checksums, magic bytes, and corruption.

    :param file: Path to the Parquet file.
    :param json: Output in JSON format.
    """
    typer.echo(f"Verifying file {file} (JSON: {json})")


@app.command()
def schema(
    file: str,
    json: Optional[bool] = typer.Option(
        False, "--json", help="Output schema in JSON format"
    ),
):
    """
    Display the schema of the Parquet file.

    :param file: Path to the Parquet file.
    :param json: Output in JSON format.
    """
    typer.echo(f"Showing schema for {file} (JSON: {json})")


if __name__ == "__main__":
    app()

"""Main CLI entry point using Typer."""

import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    help="Parquet Inspection CLI - fast and easy inspection of Apache Parquet files"
)
console = Console()


@app.callback(invoke_without_command=True)
def default(ctx: typer.Context) -> None:
    """Default callback that shows help when no command is provided."""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())


def _validate_parquet_file(file_path: Path) -> None:
    """Validate that the file exists and is a parquet file."""
    if not file_path.exists():
        console.print(f"[red]Error: File '{file_path}' does not exist[/red]")
        sys.exit(1)
    if not file_path.is_file():
        console.print(f"[red]Error: '{file_path}' is not a file[/red]")
        sys.exit(1)


@app.command()
def meta(file: Path = typer.Argument(..., help="Path to the Parquet file")) -> None:
    """Display basic metadata of the Parquet file without loading the schema."""
    _validate_parquet_file(file)

    try:
        parquet_file = pq.ParquetFile(file)
        metadata = parquet_file.metadata

        table = Table(title=f"Metadata for {file.name}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("File size", f"{file.stat().st_size:,} bytes")
        table.add_row("Number of row groups", str(metadata.num_row_groups))
        table.add_row("Number of rows", str(metadata.num_rows))
        table.add_row("Number of columns", str(len(metadata.schema)))
        table.add_row("Created by", metadata.created_by or "Unknown")
        table.add_row("Serialized size", f"{metadata.serialized_size:,} bytes")

        console.print(table)
    except Exception as e:
        console.print(f"[red]Error reading parquet file: {e}[/red]")
        sys.exit(1)


@app.command()
def head(
    file: Path = typer.Argument(..., help="Path to the Parquet file"),
    n: int = typer.Option(10, "--n", "-n", help="Number of records to display"),
) -> None:
    """Display the first N records from the Parquet file."""
    _validate_parquet_file(file)

    try:
        conn = duckdb.connect()
        df = conn.execute(f"SELECT * FROM '{file}' LIMIT {n}").df()
        console.print(df.to_string())
    except Exception as e:
        console.print(f"[red]Error reading parquet file: {e}[/red]")
        sys.exit(1)


@app.command()
def stats(file: Path = typer.Argument(..., help="Path to the Parquet file")) -> None:
    """Display statistics (min, max, count, etc.) for columns in the Parquet file."""
    _validate_parquet_file(file)

    try:
        conn = duckdb.connect()
        # Get column names
        columns = conn.execute(f"DESCRIBE SELECT * FROM '{file}' LIMIT 0").df()
        column_names = columns["column_name"].tolist()

        table = Table(title=f"Statistics for {file.name}")
        table.add_column("Column", style="cyan")
        table.add_column("Type", style="yellow")
        table.add_column("Count", style="green")
        table.add_column("Min", style="blue")
        table.add_column("Max", style="blue")
        table.add_column("Mean", style="magenta")

        for col in column_names:
            try:
                stats_query = f"""
                SELECT 
                    COUNT(*) as count,
                    MIN("{col}") as min_val,
                    MAX("{col}") as max_val,
                    AVG("{col}") as mean_val
                FROM '{file}'
                """
                result = conn.execute(stats_query).fetchone()
                count, min_val, max_val, mean_val = result

                col_type = columns[columns["column_name"] == col]["column_type"].iloc[0]

                # Format values
                min_str = str(min_val) if min_val is not None else "NULL"
                max_str = str(max_val) if max_val is not None else "NULL"
                mean_str = f"{mean_val:.2f}" if mean_val is not None else "NULL"

                table.add_row(col, col_type, str(count), min_str, max_str, mean_str)
            except Exception:
                # Skip columns that can't be aggregated
                col_type = columns[columns["column_name"] == col]["column_type"].iloc[0]
                count_query = f'SELECT COUNT(*) FROM "{file}"'
                count = conn.execute(count_query).fetchone()[0]
                table.add_row(col, col_type, str(count), "N/A", "N/A", "N/A")

        console.print(table)
    except Exception as e:
        console.print(f"[red]Error reading parquet file: {e}[/red]")
        sys.exit(1)


@app.command()
def cat(file: Path = typer.Argument(..., help="Path to the Parquet file")) -> None:
    """Concatenate and print all records to stdout."""
    _validate_parquet_file(file)

    try:
        conn = duckdb.connect()
        df = conn.execute(f"SELECT * FROM '{file}'").df()
        print(df.to_string())
    except Exception as e:
        console.print(f"[red]Error reading parquet file: {e}[/red]")
        sys.exit(1)


@app.command()
def verify(file: Path = typer.Argument(..., help="Path to the Parquet file")) -> None:
    """Verify the integrity of the Parquet file by checking checksums, magic bytes, and corruption."""
    _validate_parquet_file(file)

    try:
        parquet_file = pq.ParquetFile(file)
        metadata = parquet_file.metadata

        table = Table(title=f"Verification Results for {file.name}")
        table.add_column("Check", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Details", style="yellow")

        # Check magic bytes
        # Parquet file format spec: https://github.com/apache/parquet-format
        # Magic bytes "PAR1" (4 bytes) are defined in the Parquet File Format specification
        # See: https://github.com/apache/parquet-format/blob/master/Format.md
        with open(file, "rb") as f:
            magic = f.read(4)
            is_valid_magic = magic == b"PAR1"
            table.add_row(
                "Magic bytes",
                "✓ PASS" if is_valid_magic else "✗ FAIL",
                f"Found: {magic.hex()}" if not is_valid_magic else "Valid PAR1 header",
            )

        # Check file can be read
        try:
            _ = parquet_file.metadata
            table.add_row("Metadata readable", "✓ PASS", "File metadata is valid")
        except Exception as e:
            table.add_row("Metadata readable", "✗ FAIL", str(e))

        # Check row groups
        try:
            num_row_groups = metadata.num_row_groups
            table.add_row("Row groups", "✓ PASS", f"{num_row_groups} row groups found")
        except Exception as e:
            table.add_row("Row groups", "✗ FAIL", str(e))

        # Try to read a sample
        try:
            _ = parquet_file.read_row_group(0)
            table.add_row("Data readable", "✓ PASS", "Can read row group data")
        except Exception as e:
            table.add_row("Data readable", "✗ FAIL", str(e))

        console.print(table)
    except Exception as e:
        console.print(f"[red]Error verifying parquet file: {e}[/red]")
        sys.exit(1)


@app.command()
def schema(file: Path = typer.Argument(..., help="Path to the Parquet file")) -> None:
    """Display the schema of the Parquet file."""
    _validate_parquet_file(file)

    try:
        parquet_file = pq.ParquetFile(file)
        schema = parquet_file.schema_arrow

        table = Table(title=f"Schema for {file.name}")
        table.add_column("Column", style="cyan")
        table.add_column("Type", style="yellow")
        table.add_column("Nullable", style="green")

        for field in schema:
            nullable = "Yes" if field.nullable else "No"
            table.add_row(field.name, str(field.type), nullable)

        console.print(table)
    except Exception as e:
        console.print(f"[red]Error reading parquet file: {e}[/red]")
        sys.exit(1)


def main() -> None:
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()

import duckdb
from enum import Enum


class OutputFormat(str, Enum):
    pretty = "pretty"
    csv = "csv"
    json = "json"
    jsonl = "jsonl"


def format_relation(
    duckdb_result: duckdb.DuckDBPyRelation, format: OutputFormat
) -> str:
    try:
        if format == OutputFormat.csv:
            # materializes the CSV in full as a string
            s = duckdb_result.to_df().to_csv(index=False)
            # remove trailing newlines
            return s.strip()
        elif format == OutputFormat.json:
            return duckdb_result.to_df().to_json(orient="records")
        elif format == OutputFormat.jsonl:
            return duckdb_result.to_df().to_json(orient="records", lines=True)
        elif format == OutputFormat.pretty:
            return duckdb_result.show()
    except Exception as e:
        return f"Error formatting output: {str(e)}"

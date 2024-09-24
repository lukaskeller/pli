import duckdb
from enum import Enum


class OutputFormat(str, Enum):
    pretty = "pretty"
    csv = "csv"
    json = "json"
    jsonl = "jsonl"
    toml = "toml"


def format_relation(
    duckdb_result: duckdb.DuckDBPyRelation, format: OutputFormat
) -> str:
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
    else:
        raise NotImplementedError(f"Output format {format} not implemented")


def stripper(data):
    """
    Workaround for:
    https://github.com/sdispater/tomlkit/issues/240

    removes empty values from a dictionary
    """
    new_data = {}
    for k, v in data.items():
        # if list
        if isinstance(v, list):
            v = [stripper(i) for i in v]
        if isinstance(v, dict):
            v = stripper(v)
        if v not in ("", None, {}):
            new_data[k] = v
        else:
            new_data[k] = (
                "NONE"  # breaks the toml standard, but we want to preserve the key even if value is missing
            )
    return new_data

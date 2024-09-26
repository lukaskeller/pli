import duckdb
from enum import Enum
import json


class OutputFormat(str, Enum):
    pretty = "pretty"
    csv = "csv"
    json = "json"
    jsonl = "jsonl"


class MetaOutputFormat(str, Enum):
    json = "json"
    toml = "toml"


def format_relation(
    duckdb_result: duckdb.DuckDBPyRelation, format: OutputFormat
) -> str:
    if format == OutputFormat.csv:
        # materializes the CSV in full as a string
        # there are probably more efficient ways to do this with duckdb
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


def tomlizer(data):
    """
    Workaround for:
    https://github.com/sdispater/tomlkit/issues/240

    replaces empty values with "NONE" to preserve the existence of the key and to ensure convertability ofdict to toml
    """
    new_data = {}
    for k, v in data.items():
        # if list
        if isinstance(v, list):
            v = [tomlizer(i) for i in v]
        if isinstance(v, dict):
            v = tomlizer(v)
        if v not in ("", None, {}):
            new_data[k] = v
        else:
            new_data[k] = (
                "NONE"  # breaks the toml standard, but we want to preserve the key even if value is missing
            )
    return new_data


# python dicts can have bytes as keys, but the built-in json module can't serialize them
# we graciously assume that the bytes are utf-8 encoded strings
# example
# {b'pandas': b'{"index_columns": [{"kind": "range", "name": null, "start": 0, "stop": 100, "step": 1...on": "17.0.0"}, "pandas_version": "2.2.3"}'}
class BytesDump(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode()
        return json.JSONEncoder.default(self, obj)

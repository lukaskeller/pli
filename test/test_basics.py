from tempfile import NamedTemporaryFile, NamedTemporaryFile
import json

import pytest

from typer.testing import CliRunner

from cli.main import app
from fixtures.file_gen import (
    pandas_dataframe,
    pyarrow_parquet_file,
    duckdb_parquet_file,
    pyarrow_parquet_file_path,
)

runner = CliRunner()


def test_head(pyarrow_parquet_file_path):
    f = pyarrow_parquet_file_path
    result = runner.invoke(app, ["head", f.name])
    assert result.exit_code == 0
    assert "random_ints" in result.stdout.split("\n")[1]
    assert "random_floats" in result.stdout.split("\n")[1]
    assert "A_B_or_C" in result.stdout.split("\n")[1]


def test_head_as_jsonl(pyarrow_parquet_file_path, pandas_dataframe):
    f = pyarrow_parquet_file_path
    result = runner.invoke(app, ["head", f.name, "--format", "jsonl"])
    assert result.exit_code == 0
    # assert 10 rows
    assert len(result.stdout.split("\n")) == 12  # 10 rows + header + empty line
    assert "random_ints" in result.stdout.split("\n")[0]
    assert "random_floats" in result.stdout.split("\n")[0]
    assert "A_B_or_C" in result.stdout.split("\n")[0]

    # also in the first row
    assert "random_ints" in result.stdout.split("\n")[1]
    assert "random_floats" in result.stdout.split("\n")[1]
    assert "A_B_or_C" in result.stdout.split("\n")[1]

    # read line 7 and check if it is a valid json
    l = result.stdout.split("\n")[7]
    json.loads(l)  # should not raise an exception
    # verify against 7th row of the dataframe
    assert json.loads(l) == pytest.approx(pandas_dataframe.iloc[7].to_dict())


def test_head_as_json(pyarrow_parquet_file, pandas_dataframe):
    with NamedTemporaryFile(suffix=".parquet") as f:
        f.write(pyarrow_parquet_file.read())
        f.seek(0)
        print(f.name)
        result = runner.invoke(app, ["head", f.name, "--format", "json"])
        assert result.exit_code == 0

        assert "random_ints" in result.stdout.split("\n")[0]
        assert "random_floats" in result.stdout.split("\n")[0]
        assert "A_B_or_C" in result.stdout.split("\n")[0]

        l = result.stdout
        json.loads(l)  # should not raise an exception
        assert json.loads(l)[0] == pytest.approx(pandas_dataframe.iloc[0].to_dict())


def test_head_as_csv(pyarrow_parquet_file):
    with NamedTemporaryFile(suffix=".parquet") as f:
        f.write(pyarrow_parquet_file.read())
        f.seek(0)
        print(f.name)
        result = runner.invoke(app, ["head", f.name, "--format", "csv"])
        assert result.exit_code == 0
        # assert 10 rows
        assert len(result.stdout.split("\n")) == 12  # 10 rows + header + empty line
        assert "random_ints" in result.stdout.split("\n")[0]
        assert "random_floats" in result.stdout.split("\n")[0]
        assert "A_B_or_C" in result.stdout.split("\n")[0]

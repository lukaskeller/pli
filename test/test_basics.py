import json
import os

import pytest

from typer.testing import CliRunner

from cli.main import app
from fixtures.file_gen import pandas_dataframe, temp_dir, make_test_files  # noqa: F401

runner = CliRunner()


def test_head_pyarrow(temp_dir):
    result = runner.invoke(
        app, ["head", os.path.join(temp_dir, "pyarrow_plain.parquet")]
    )
    assert result.exit_code == 0
    first_stdout_line = result.stdout.split("\n")[1]
    assert "random_ints" in first_stdout_line
    assert "random_floats" in first_stdout_line
    assert "A_B_or_C" in first_stdout_line


def test_head_duckdb(temp_dir):
    result = runner.invoke(
        app, ["head", os.path.join(temp_dir, "duckdb_plain.parquet")]
    )

    assert result.exit_code == 0
    first_stdout_line = result.stdout.split("\n")[1]
    assert "random_ints" in first_stdout_line
    assert "random_floats" in first_stdout_line
    assert "A_B_or_C" in first_stdout_line


def test_head_as_jsonl(temp_dir, pandas_dataframe):
    result = runner.invoke(
        app,
        ["head", os.path.join(temp_dir, "pyarrow_plain.parquet"), "--format", "jsonl"],
    )
    assert result.exit_code == 0
    # assert 10 rows
    assert len(result.stdout.split("\n")) == 12  # 10 rows + header + empty line
    row = json.loads(result.stdout.split("\n")[7])  # load 7th row as json
    assert "random_ints" in row
    assert "random_floats" in row
    assert "A_B_or_C" in row
    assert row == pytest.approx(pandas_dataframe.iloc[7].to_dict())


# @pytest.mark.usefixtures("pyarrow_parquet_file_path")
# @pytest.mark.usefixtures("pandas_dataframe")
# def test_head_as_jsonl():
#     f = pyarrow_parquet_file_path()
#     result = runner.invoke(app, ["head", f.name, "--format", "jsonl"])
#     assert result.exit_code == 0
#     # assert 10 rows
#     assert len(result.stdout.split("\n")) == 12  # 10 rows + header + empty line
#     assert "random_ints" in result.stdout.split("\n")[0]
#     assert "random_floats" in result.stdout.split("\n")[0]
#     assert "A_B_or_C" in result.stdout.split("\n")[0]

#     # also in the first row
#     assert "random_ints" in result.stdout.split("\n")[1]
#     assert "random_floats" in result.stdout.split("\n")[1]
#     assert "A_B_or_C" in result.stdout.split("\n")[1]

#     # read line 7 and check if it is a valid json
#     l = result.stdout.split("\n")[7]
#     json.loads(l)  # should not raise an exception
#     # verify against 7th row of the dataframe
#     assert json.loads(l) == pytest.approx(pandas_dataframe.iloc[7].to_dict())


# @pytest.mark.usefixtures("pyarrow_parquet_file_path")
# @pytest.mark.usefixtures("pandas_dataframe")
# def test_head_as_json():
#     with NamedTemporaryFile(suffix=".parquet") as f:
#         f.write(pyarrow_parquet_file().read())
#         f.seek(0)
#         print(f.name)
#         result = runner.invoke(app, ["head", f.name, "--format", "json"])
#         assert result.exit_code == 0

#         assert "random_ints" in result.stdout.split("\n")[0]
#         assert "random_floats" in result.stdout.split("\n")[0]
#         assert "A_B_or_C" in result.stdout.split("\n")[0]

#         l = result.stdout
#         json.loads(l)  # should not raise an exception
#         assert json.loads(l)[0] == pytest.approx(pandas_dataframe.iloc[0].to_dict())


# @pytest.mark.usefixtures("pyarrow_parquet_file_path")
# def test_head_as_csv():
#     with NamedTemporaryFile(suffix=".parquet") as f:
#         f.write(pyarrow_parquet_file.read())
#         f.seek(0)
#         print(f.name)
#         result = runner.invoke(app, ["head", f.name, "--format", "csv"])
#         assert result.exit_code == 0
#         # assert 10 rows
#         assert len(result.stdout.split("\n")) == 12  # 10 rows + header + empty line
#         assert "random_ints" in result.stdout.split("\n")[0]
#         assert "random_floats" in result.stdout.split("\n")[0]
#         assert "A_B_or_C" in result.stdout.split("\n")[0]

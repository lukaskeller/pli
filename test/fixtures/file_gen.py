import os
import pytest
import pandas as pd
import numpy as np
import duckdb  # type: ignore
import tempfile


# fixture with lifecycle for entire run
# https://docs.pytest.org/en/6.2.x/fixture.html#fixture-lifecycle
@pytest.fixture(scope="session")
def pandas_dataframe():
    # Generate random data
    num_rows = 100
    data = {
        "random_ints": np.random.randint(0, 100, size=num_rows),
        "random_floats": np.random.random(size=num_rows),
        "A_B_or_C": np.random.choice(["A", "B", "C"], size=num_rows),
    }
    return pd.DataFrame(data)


@pytest.fixture(scope="session")
def temp_dir():
    with tempfile.TemporaryDirectory() as newpath:
        yield newpath


# @pytest.mark.usefixtures("inside_temp_directory")
@pytest.fixture(scope="session")
def make_test_files(pandas_dataframe: pd.DataFrame, temp_dir: str):
    # print(temp_dir)
    # Use pyarrow to write a parquet file
    pandas_dataframe.to_parquet(
        os.path.join(temp_dir, "pyarrow_plain.parquet"), engine="pyarrow"
    )

    # Use duckdb to write a parquet file
    ddb_file = os.path.join(temp_dir, "duckdb_plain.parquet")
    duckdb.execute(
        f"COPY pandas_dataframe TO '{ddb_file}' (FORMAT 'parquet', CODEC 'zstd')"
    )

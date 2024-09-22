import pytest
import pandas as pd
import numpy as np
import duckdb  # type: ignore
import tempfile
import pyarrow as pa
from io import BytesIO

import pyarrow.parquet as pq


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


@pytest.fixture(scope="function")
def pyarrow_parquet_file(pandas_dataframe: pd.DataFrame):
    df = pandas_dataframe
    # Convert DataFrame to Parquet and write to BytesIO
    buffer = BytesIO()
    df.to_parquet(buffer)
    buffer.seek(0)

    return buffer


# this fixture should use namedtempfile and yield the file object
@pytest.fixture(scope="session")
def pyarrow_parquet_file_path(pandas_dataframe: pd.DataFrame):
    df = pandas_dataframe
    # use context manager with tempfile to create a temporary file to write to
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        # write the parquet file
        df.to_parquet(f.name)
        yield f


# now create a parquetfile with duckdb
@pytest.fixture(scope="session")
def duckdb_parquet_file(pandas_dataframe: pd.DataFrame):
    df = pandas_dataframe

    # use context manager with tempfile to create a temporary file to write to
    with tempfile.TemporaryFile() as f:
        # create a duckdb connection
        con = duckdb.connect(database=":memory:")
        con.execute(
            f"""
        COPY df TO '{f.name}' (FORMAT 'parquet', CODEC 'zstd')
        """
        )
        # read the file
        f.seek(0)
        buffer = BytesIO(f.read())

    return buffer

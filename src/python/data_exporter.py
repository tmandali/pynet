from collections.abc import Buffer
from io import BytesIO
import polars as pl
import connectorx as cx

def to_polar(db_uri: str, query: str) -> pl.DataFrame:   
    return cx.read_sql(db_uri, query, return_type="polars")

def to_parquet(db_uri: str, query: str) -> Buffer:   
    df = to_polar(db_uri, query)
    with BytesIO() as out_stream:
        df.write_parquet(out_stream, compression="zstd", use_pyarrow=True, pyarrow_options={"row_group_size": 100_000})
        return out_stream.getvalue()

def to_parquet_dataset(query: str, dataset: dict[str, pl.DataFrame]) -> Buffer:   
    ctx = pl.SQLContext()
    for name, df in dataset.items():
        ctx.register(name, df)

    with BytesIO() as out_stream:
        ctx.execute(query).collect().write_parquet(out_stream, compression="zstd", use_pyarrow=True, pyarrow_options={"row_group_size": 100_000})
        return out_stream.getvalue()

if __name__ == "__main__":
    db_uri = "mssql://sa:Passw@rd@localhost:1433/TestDb?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
    query = "SELECT * FROM users"
    # buffer = to_parquet(db_uri, query)
    buffer = to_parquet(query, dataset={"users": to_polar(db_uri, query)})

    with open("output.parquet", "wb") as f:
        f.write(buffer)
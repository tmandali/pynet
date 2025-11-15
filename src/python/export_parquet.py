from typing_extensions import Buffer
import polars as pl
import logging
from io import BytesIO
import duckdb
from urllib import parse
from typing import List, Tuple, Optional
import sqlalchemy
import pyarrow as pa
import pyarrow.parquet as pq
import connectorx as cx

# def pars_quote_plus(password: str) -> str:
#     return parse.quote_plus(password)

def to_parquet(query: str, dataset: List[Tuple[str, str, str]]) -> Buffer:
    
    for item in dataset:
        name, db_uri, source = item
        duckdb.register(name, pl.read_database_uri(
            query=source,
            uri=db_uri,
            engine="connectorx"))

    with BytesIO() as out_stream:
        duckdb.sql(query).pl().write_parquet(
            out_stream,
            compression="zstd",
            use_pyarrow=True,
            pyarrow_options={"row_group_size": 100_000}
        )

        return out_stream.getvalue()
    
def to_parquet(db_uri: str, query: str) -> Buffer:
    
    # sql_text = sqlalchemy.text(
    # """ 
    # SELECT CONVERT(DATE, Date) AS [Date]
    # , Exchange_Rate / 100 AS [Exchange_Rate] FROM [dbo].[my_table] WHERE Date >= :Date 
    # """
    # ).bindparams(sqlalchemy.bindparam("Date", value="2022-05-31"))
    # compiled_query = str(sql_text.compile(compile_kwargs={"literal_binds": True})) 

    # conn = 'mssql://username:password@server:port/database?encrypt=true&trusted_connection=true'         # connection token
    # query = 'SELECT * FROM table'                                   # query string
   
    df: pl.DataFrame = cx.read_sql(db_uri, query, return_type="polars")

    with BytesIO() as out_stream:
        df.write_parquet(out_stream, compression="zstd", use_pyarrow=True, pyarrow_options={"row_group_size": 100_000})
        return out_stream.getvalue()
       # read data from MsSQL

    with BytesIO() as out_stream:
        pl.read_database_uri(
            query=query,
            uri=db_uri,
            engine="connectorx").write_parquet(
                out_stream,
                compression="zstd",
                use_pyarrow=True,
                pyarrow_options={"row_group_size": 100_000}
            )

        return out_stream.getvalue()

    # con = duckdb.connect("file.db")
    # duckdb.register("users", df)
    # df = duckdb.sql("select * from users").pl()
    # con.execute("CREATE TABLE users_table AS SELECT * FROM users")

    # BytesIO nesnesi oluştur
    # outStream = BytesIO()
    # workbook = Workbook(outStream, {'in_memory': True})

    # MAX_ROWS_PER_SHEET = 1_048_575  # Excel'in gerçek maksimum satır sayısı
    # total_rows = len(df)

    # # Kaç sayfa gerektiğini hesapla
    # num_sheets = (total_rows + MAX_ROWS_PER_SHEET - 1) // MAX_ROWS_PER_SHEET

    # # Her sayfa için döngü
    # for sheet_num in range(num_sheets):
    #     start_idx = sheet_num * MAX_ROWS_PER_SHEET
    #     end_idx = min(start_idx + MAX_ROWS_PER_SHEET, total_rows)

    #     # Bu sayfa için veri dilimini al
    #     df_slice = df.slice(start_idx, end_idx - start_idx)

    #     # Yeni sayfa oluştur
    #     worksheet = workbook.add_worksheet(f'Sayfa{sheet_num + 1}')

    #     # Veriyi sayfaya yaz
    #     df_slice.write_excel(
    #         workbook=workbook,
    #         worksheet=worksheet,
    #         position="A1",
    #         autofit=True
    #     )
    #     logging.info(f"Sayfa{sheet_num + 1}'e {len(df_slice)} satır yazıldı.")

    # workbook.close()

        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    kullanici = "sa"
    parola = parse.quote_plus("Passw@rd")
    host = "localhost"
    port = 1433
    veritabani = "TestDb"
    
    DB_URI = f"mssql+pyodbc://{kullanici}:{parola}@{host}:{port}/{veritabani}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
    source_query = "SELECT * FROM Users"
    
    buffer = to_parquet(DB_URI, source_query)

    with open("output_mssql.parquet", "wb") as f:
        f.write(buffer)

    logging.info("Parquet dosyası başarıyla kaydedildi.")
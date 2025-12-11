import os
import tempfile
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import urllib
import time
import polars as pl
from typing import Iterator

# --- AYARLAR ---
SERVER = 'TEMAOLTP'
DATABASE = 'Store7'
colums = ['ID','UrunID','UrunKod','Renk','Beden','Boy','Tarih','Line','NumuneVaryantSira','SergiEkipmanRef','SizeRef','UrunOptionRef','UrunOptionSizeRef','UrunOptionAsortiRef','BedenBoyRef','Konsinye','AxaAktarilsinmi','UrunSKULevelRef','LcwArticleCode','Agirlik','UrunKoliIcerikTipref']

OUTPUT_FILE = "urun.parquet"
BATCH_SIZE = 100_000       

connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
params = urllib.parse.quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def mssql_to_parquet_change(hist_id: int) -> Iterator[pl.DataFrame]:    
    
    CHANGE_QUERY = f"""
        SELECT {', '.join(colums)}, Hist_ID, Hist_Islem 
        FROM tb_Urun_Hist 
        WHERE Hist_ID > :Hist_ID
        ORDER BY Hist_ID
    """ 
    
    for df in pl.read_database(
            query=CHANGE_QUERY, 
            connection=engine.execution_options(stream_results=True), 
            iter_batches=True, 
            batch_size=BATCH_SIZE, 
            execute_options={"parameters": {"Hist_ID": hist_id}}):
        yield df


def mssql_to_parquet_init():
    
    start_time = time.time()
    writer = None
    row_count = 0

    INIT_QUERY = f"""
        DECLARE @Hist_ID BIGINT
        SELECT @Hist_ID = MAX(Hist_ID) FROM tb_Urun_Hist
        SELECT {', '.join(colums)}, @Hist_ID as Hist_ID, Hist_Islem = 1 
        FROM tb_Urun
    """ 

    try:
        for i, df in enumerate(pl.read_database(
            query=INIT_QUERY,
            connection=engine.execution_options(stream_results=True),
            iter_batches=True,
            batch_size=BATCH_SIZE
        )):
            
            table = df.to_arrow()
            
            if writer is None:
                writer = pq.ParquetWriter(OUTPUT_FILE, table.schema)
            
            writer.write_table(table)
            
            row_count += len(df)
            print(f"Parça {i+1} işlendi. Toplam: {row_count} satır.")

    except Exception as e:
        print(f"\n!!! HATA !!!: {e}")

    finally:
        # Dosyayı kapatmak zorunludur, yoksa dosya bozuk olur.
        if writer:
            writer.close()
            print(f"--- İşlem Tamamlandı! Süre: {(time.time() - start_time)/60:.2f} dk ---")
        engine.dispose() 

if __name__ == "__main__":
    #if not os.path.exists(OUTPUT_FILE):
    #   mssql_to_parquet_init()
    
    start_time = time.time()
    print(f"--- İşlem Başladı! Süre: {(time.time() - start_time)/60:.2f} dk ---")
    hist_id = pl.scan_parquet(OUTPUT_FILE).select(pl.col("Hist_ID")).max().collect().item()
    print(hist_id)

    writer = None
    with tempfile.NamedTemporaryFile(mode='w+b') as change_file:
        for change_df in mssql_to_parquet_change(hist_id):
            table = change_df.to_arrow()

            if writer is None:
                writer = pq.ParquetWriter(change_file, schema=table.schema)

            writer.write_table(table)

        if writer:
            writer.close()
            change_file.seek(0)
            change_df = pl.scan_parquet(change_file).sort("Hist_ID").unique(subset=["ID"], keep="last")
            parquet_writer = None
            with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as parquet_file:
                
                for change_batch in change_df.filter(pl.col("Hist_Islem") > 0).collect_batches(chunk_size=BATCH_SIZE):
                    table = change_batch.to_arrow()
                    if parquet_writer is None:
                        parquet_writer = pq.ParquetWriter(parquet_file, schema=table.schema)               
                    parquet_writer.write_table(table)

                if parquet_writer:
                    for batch in pl.scan_parquet(OUTPUT_FILE).join(change_df, on="ID", how="anti").collect_batches(chunk_size=BATCH_SIZE):
                        parquet_writer.write(batch.to_arrow())

                    parquet_writer.close()
                    parquet_file.close()
                    os.replace(OUTPUT_FILE, OUTPUT_FILE + ".backup")
                    os.replace(parquet_file.name, OUTPUT_FILE)
        
    print(f"--- İşlem Tamamlandı! Süre: {(time.time() - start_time)/60:.2f} dk ---")
    
    #with tempfile.NamedTemporaryFile(mode='w+b') as change_file:

    #select * from urun.parquet QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY Hist_ID DESC) = 1;
    #SELECT DISTINCT ON (ID) * FROM urun.parquet  ORDER BY ID, Hist_ID DESC;

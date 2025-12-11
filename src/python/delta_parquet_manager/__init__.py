import logging
import os
from typing import Any, Iterable
from urllib.parse import urlparse
import uuid
import polars as pl
from pypika import Field, MSSQLQuery, Table
import connectorx as cx
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)
if not logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def read_database_partition(db_uri:str, table_name:str, partition_on:str, columns:str|list[str]='*', last_value:Any=None, limit:int = 100_000, partition_num:int = 8) -> Iterable[tuple[Any, pl.DataFrame]]:
    while True:
        table = Table(table_name)
        table_query = MSSQLQuery.from_(table).select(Field(partition_on))
        if last_value is not None:
            table_query = table_query.where(Field(partition_on).gt(last_value))
        table_query = table_query.orderby(1).limit(limit).get_sql()
        
        range_query = f"""           
            WITH Part AS ({table_query}),
            Range AS (SELECT NTILE({partition_num}) OVER (ORDER BY {partition_on}) as Part, {partition_on} FROM Part)
            SELECT Part, MIN({partition_on}) as BeginPart, MAX({partition_on}) as EndPart FROM Range GROUP BY Part
        """
        range_df = pl.read_database_uri(range_query, db_uri).iter_rows(named=True)
        part_query_list: list[str] = []
        last_value = None
        for range in range_df:
            part_query = MSSQLQuery.from_(table)\
                .select(*(columns if isinstance(columns, str) else columns))\
                .where(Field(partition_on)\
                .between(range['BeginPart'], range['EndPart'])).get_sql()
            part_query_list.append(part_query)
            last_value = range['EndPart']

        if len(part_query_list) == 0:
            break

        part_df = pl.read_database_uri(part_query_list, db_uri)
        yield (last_value, part_df)  
        
def _read_database_mssql(db_uri:str, query:str, order_by_column:str, last_id:Any, limit:int, partition:int) -> pl.DataFrame:   

    resume = ""
    if last_id is not None:
        resume = f"WHERE {order_by_column} > '{last_id}'"

    range_query = f"""           
        WITH Part AS (SELECT TOP {limit} {order_by_column} FROM ({query}) source {resume} ORDER BY 1),
        Range AS (SELECT NTILE({partition}) OVER (ORDER BY {order_by_column}) as Part, {order_by_column} FROM Part)
        SELECT Part, MIN({order_by_column}) as BeginPart, MAX({order_by_column}) as EndPart FROM Range GROUP BY Part
    """

    range = pl.read_database_uri(range_query, db_uri).to_dicts()
    if range is None or len(range) == 0:
        return None

    part_list: list[str] = [f"""
        SELECT * FROM ({query}) part WHERE {order_by_column} BETWEEN '{range["BeginPart"]}' AND '{range["EndPart"]}'
        """ for range in range]

    df = pl.read_database_uri(part_list, db_uri)
    if df.is_empty():
        return None

    return df                

def read_database_part(db_uri:str, query:str, order_by_column:str, last_id:Any=None, limit:int=1_000_000, partition:int=8):     
    if partition <= 0 or partition is None:
        partition = 1

    parsed_db_uri = urlparse(db_uri)
    if parsed_db_uri.scheme.lower() == "mssql":
        read_database_func = _read_database_mssql
    else:
        raise ValueError(f"Database type {parsed_db_uri.scheme} is not supported")

    while True:
        df = read_database_func(db_uri, query, order_by_column, last_id, limit, partition)

        if df is None:
            break

        last_id = df[order_by_column].max()
        yield df

def write_delta(delta_table:str, db_uri:str, query:str, order_by_column:str, limit:int=1_000_000, partition:int=8) -> pl.DataFrame:
  
    last_id = None
    if os.path.exists(delta_table):
       last_id = pl.read_delta(delta_table)[order_by_column].max()
    
    reader = read_database_part(db_uri, query, order_by_column, last_id, limit, partition)
    for df in reader:
        logger.info(f"Writing delta table: {delta_table} with height: {df.height}")
        df.write_delta(
            target=delta_table, 
            mode="append")

    logger.info(f"Delta table write completed")

def write_delta_merge(delta_table:str, db_uri:str, query:str,  order_by_column:str, key_column:str = None, limit:int=1_000_000, partition:int=8) -> pl.DataFrame:
    last_id = None
    if os.path.exists(delta_table):
       last_id = pl.read_delta(delta_table)[order_by_column].max()

    key_column = key_column or order_by_column

    reader = read_database_part(db_uri, query, order_by_column, last_id, limit, partition)
    for df in reader:
        df = df.filter(
            pl.col(order_by_column) == pl.col(order_by_column).max().over(key_column))
 
        logger.info(f"Writing delta table merge: {delta_table} with height: {df.height}")
        df.write_delta(
            target=delta_table, 
            mode="merge",
            delta_merge_options={
                "predicate": f"s.{key_column} = t.{key_column}",  
                "source_alias": "s",         
                "target_alias": "t",   
            }).when_matched_update_all().when_not_matched_insert_all().execute()

    logger.info(f"Delta table merge completed")

if __name__ == "__main__":
    logger.setLevel(logging.INFO) 

    # from pyarrow import fs
    # localfs = fs.LocalFileSystem()
    # partitioned_dir_content = localfs.get_file_info(fs.FileSelector("./partitioned", recursive=True))
    # files = sorted((f.path for f in partitioned_dir_content if f.type == fs.FileType.File))

    # for file in files:
    #     print(file)

    # s3 = fs.SubTreeFileSystem(
    # "ursa-labs-taxi-data",
    # fs.S3FileSystem(region="us-east-2", anonymous=True))

    # for entry in s3.get_file_info(fs.FileSelector("2011", recursive=True)):
    #     if entry.type == fs.FileType.File:
    #         print(entry.path)

    db_uri = "mssql://temaoltp/Store7?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true"
    folder_path = os.path.normpath("./data/urun")

    # import pyodbc
    # import pandas as pd

    # from adbc_driver_postgresql import dbapi  
    # with dbapi.connect('mssql://testoltp/Store7?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true') as conn:  # doctest:+SKIP
    #    df = pd.read_sql('SELECT * FROM tb_Urun WHERE ID >= 30000000 AND ID <= 31000000', conn)

    # connection_string = (
    #     'DRIVER={ODBC Driver 17 for SQL Server};'
    #     'SERVER=TEMAOLTP;'  # Örneğin: 'localhost', 'SUNUCU_ADI\ORNEK_INST'
    #     'DATABASE=Store7;'
    #     'Trusted_Connection=yes;'
    # )
    # conn = pyodbc.connect(connection_string)
    # df = pd.read_sql("SELECT * FROM tb_Urun WHERE ID >= 30000000 AND ID <= 31000000", conn)
    # print(df.describe())
    # print(len(df))


    # from pyarrow import fs
    # localfs = fs.LocalFileSystem()
    # partitioned_dir_content = localfs.get_file_info(fs.FileSelector("./data/urun", recursive=True))
    # files = sorted((f.path for f in partitioned_dir_content if f.type == fs.FileType.File))

    # for file in files:
    #     print(file)

    import tempfile
    import duckdb

    chunk_size = 10_000_000    
    range = 0
    last_value = None
    table_name = "tb_Urun"
    partition_num = 8
    partition_on = "ID"
    colums = ['ID','UrunID','UrunKod','Renk','Beden','Boy','Tarih','Line','NumuneVaryantSira','SergiEkipmanRef','SizeRef','UrunOptionRef','UrunOptionSizeRef','UrunOptionAsortiRef','BedenBoyRef','Konsinye','AxaAktarilsinmi','UrunSKULevelRef','LcwArticleCode','Agirlik','UrunKoliIcerikTipref','Hist_ID','Hist_Islem']

    with duckdb.connect("datadb.db") as con:
        con.sql("ATTACH 'ducklake:metadata.ducklake' AS data (DATA_PATH 'data_files')")
        con.sql("USE data")
        #duck_con.execute(f"drop table if exists {table_name}")

        while True:
            if (os.path.exists(f"data_files/main/{table_name}")):    
                result = con.sql(f"SELECT max(Hist_ID) FROM {table_name}").fetchone()
                hist_id = result[0]

            table = Table(table_name + "_Hist")
            table_query = MSSQLQuery.from_(table).select(Field("Hist_ID"))
            if hist_id is not None:
                table_query = table_query.where(Field("Hist_ID").gt(hist_id))
            table_query = table_query.orderby(1).limit(chunk_size).get_sql()            

            range_query = f"""           
                WITH Part AS ({table_query}),
                Range AS (SELECT NTILE({partition_num}) OVER (ORDER BY Hist_ID) as Part, Hist_ID FROM Part)
                SELECT Part, MIN(Hist_ID) as BeginPart, MAX(Hist_ID) as EndPart FROM Range GROUP BY Part
            """

            range_list =  pl.read_database_uri(range_query, db_uri).to_dicts()  
            query_list = []
            for range_dict in range_list:  
                query_list.append(
                    MSSQLQuery.from_(table)
                        .select(*colums)
                        .where(Field('Hist_ID').between(range_dict["BeginPart"], range_dict["EndPart"]))
                        .get_sql())

            if len(query_list) == 0:
                break
            
            # reader: pa.ipc.RecordBatchStreamReader = cx.read_sql(
            #     db_uri, 
            #     query_list,  
            #     return_type="arrow_stream")

            df = cx.read_sql(
                db_uri, 
                query_list,  
                return_type="polars").sort("Hist_ID").unique(subset=["ID"], keep="last")
            
            merge_sql = f"""
                MERGE INTO {table_name}
                USING (
                    SELECT * FROM df
                ) AS upserts
                ON (upserts.{partition_on} = {table_name}.{partition_on})
                WHEN MATCHED THEN UPDATE
                WHEN NOT MATCHED THEN INSERT;
            """

            con.sql(merge_sql)
            con.sql(f"DELETE FROM {table_name} WHERE Hist_Islem=0")
            break

        while True:    
            if (os.path.exists(f"data_files/main/{table_name}")):    
                result = con.sql(f"SELECT max(ID) FROM {table_name}").fetchone()
                last_value = result[0]
                
            table = Table(table_name)
            table_query = MSSQLQuery.from_(table).select(Field("ID"))
            if last_value is not None:
                table_query = table_query.where(Field("ID").gt(last_value))
            table_query = table_query.orderby(1).limit(chunk_size).get_sql()
        
            range_query = f"""           
                WITH Part AS ({table_query}),
                Range AS (SELECT NTILE({partition_num}) OVER (ORDER BY {partition_on}) as Part, {partition_on} FROM Part)
                SELECT Part, MIN({partition_on}) as BeginPart, MAX({partition_on}) as EndPart FROM Range GROUP BY Part
            """

            range_list =  pl.read_database_uri(range_query, db_uri).to_dicts()  
            query_list = []
            for range_dict in range_list:  
                query_list.append(
                    MSSQLQuery.from_(table)
                        .select(*colums)
                        .select(Field('Hist_ID').eq(603763973))
                        .select(Field('Hist_Islem').eq(1))
                        .where(Field(partition_on).between(range_dict["BeginPart"], range_dict["EndPart"]))
                        .get_sql())

            if len(query_list) == 0:
                break

            reader: pa.ipc.RecordBatchStreamReader = cx.read_sql(
                db_uri, 
                query_list,  
                return_type="arrow_stream")
            
            if last_value is None:
                con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM reader")
                #con.sql(f"ALTER TABLE {table_name} ADD COLUMN Hist_ID INTEGER DEFAULT 0")
                #con.sql(f"ALTER TABLE {table_name} ADD COLUMN Hist_Islem INTEGER DEFAULT 1")
            else:
                con.sql(f"INSERT INTO {table_name} SELECT * FROM reader")           

exit()
        
    #     reader: pa.ipc.RecordBatchStreamReader = cx.read_sql(
    #         db_uri, 
    #         f"SELECT * FROM tb_Urun",  
    #         return_type="arrow_stream", 
    #         partition_on="ID", 
    #         partition_range=[range + 1, range + chunk_size], 
    #         partition_num=8,
    #         )    
        
    #     # with pa.ipc.new_file(sink, batch.schema) as writer:
    #     file_name = uuid.uuid4()
    #     with pq.ParquetWriter(f"data//urun/{file_name}.tmp", schema=reader.schema) as writer:
    #         rows = 0
    #         #while (batch := reader.read_next_batch()):
    #         for i ,batch in enumerate(reader):                        
    #             rows += batch.num_rows
    #             print(f"Rows: {rows}", end='\r')
    #             writer.write_batch(batch)
            
    #         writer.close()
    #         if rows == 0:   
    #             os.remove(f"data//urun/{file_name}.tmp")
    #             break
        
    #     os.replace(f"data//urun/{file_name}.tmp", f"data//urun/{file_name}.parquet")
    #     range += chunk_size            

    #         # for i ,batch in enumerate(reader):                        
    #         #     writer.write_batch(batch)               
    #         #     logger.info(f"Batch {i}: Max ID: {pc.max(batch["ID"]).as_py()} Rows: {batch.num_rows}")        
    # exit()
    # hist_id = None
    # last_value = None

    # if os.path.exists(folder_path):
    #     df = pl.scan_parquet(folder_path)
    #     hist_id = df.select(pl.col("Hist_ID").max()).collect()["Hist_ID"].item()
    #     last_value = df.select(pl.col("ID").max()).collect()["ID"].item()
   
    # if not hist_id:
    #     hist_id = pl.read_database_uri("SELECT max(Hist_ID) as current_history_id FROM tb_Urun_Hist", db_uri)["current_history_id"].item()

    # if not hist_id:
    #     hist_id = 0

    # logger.info(f"Sync started")

    # for max_value, df in read_database_partition(db_uri, "tb_Urun", "ID", last_value=last_value, limit=2_000_000):
    #     df = df.with_columns(
    #         (pl.col("ID") // 1_000_000).alias("bucket_id"),
    #         pl.lit(hist_id).cast(pl.Int64).alias("Hist_ID"),
    #         pl.lit(1).cast(pl.Int16).alias("Hist_Islem")
    #     )
    #     partitions = df.partition_by("bucket_id", as_dict=True)
    
    #     for bucket_val, df_updates in partitions.items():
    #         file_name = f"part_{bucket_val[0]}.parquet"
    #         file_path = os.path.normpath(os.path.join(folder_path, file_name))

    #         df_updates = df_updates.drop("bucket_id")

    #         if os.path.exists(file_path):
    #             df_current = pl.read_parquet(file_path)
    #             df_updates = pl.concat([df_current, df_updates])
    #             df_updates = df_updates.unique(subset=["ID"], keep="last")
         
    #         df_updates.write_parquet(file_path, mkdir=True)

    #     # df = df.with_columns(
    #     #   pl.lit(hist_id).cast(pl.Int64).alias("Hist_ID"),
    #     #   pl.lit(1).cast(pl.Int16).alias("Hist_Islem"))
    #     # df.write_delta(delta_table, mode="append")
    #     # print(f"Init Max value: {max_value}, Height: {df.height}")
    #     # del df
        
    # logger.info(f"Init completed")

    # for max_value, df in read_database_partition(db_uri, "tb_Urun_Hist", "Hist_ID", last_value=hist_id, limit=100_000):
    #     df = df.with_columns(
    #         (pl.col("ID") // 1_000_000).alias("bucket_id"),
    #     )
    #     partitions = df.partition_by("bucket_id", as_dict=True)
    
    #     for bucket_val, df_updates in partitions.items():
    #         file_name = f"part_{bucket_val[0]}.parquet"
    #         file_path = os.path.normpath(os.path.join(folder_path, file_name))

    #         df_updates = df_updates.drop("bucket_id")

    #         if os.path.exists(file_path):
    #             df_current = pl.read_parquet(file_path)
    #             df_updates = pl.concat([df_current, df_updates])
    #             df_updates = df_updates.sort("Hist_ID").unique(subset=["ID"], keep="last")
         
    #         df_updates.write_parquet(file_path, mkdir=True)

        # df.write_delta(
        #     target=delta_table, 
        #     mode="merge",
        #     delta_merge_options={
        #         "predicate": f"s.ID = t.ID",  
        #         "source_alias": "s",         
        #         "target_alias": "t",   
        #     }).when_matched_update_all().when_not_matched_insert_all().execute()

    #     df.write_delta(target=delta_table, mode="append")
    #     df = df.sort("Hist_ID").unique(subset=["ID"], keep="last")
    #     logger.info(f"Writing delta table: {delta_table} with height: {df.height}")
    #     del df
    
    # delta_df = pl.read_delta(delta_table)
    # delta_df = delta_df.sort("Hist_ID").unique(subset=["ID"], keep="last")
    # delta_df.write_delta(delta_table, mode="overwrite")
    # del delta_df

    # logger.info(f"Hist completed")
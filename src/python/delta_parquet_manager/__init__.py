import datetime
import logging
import os
import shutil
from typing import Any, Callable
from urllib.parse import urlparse
import polars as pl

logger = logging.getLogger(__name__)
if not logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

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

def read_database_part(db_uri:str, query:str, order_by_column:str, reinit=False, limit:int=1_000_000, partition:int=8):     
    if partition <= 0 or partition is None:
        partition = 1
    
    last_id = None    
    if os.path.exists(delta_table):
        if reinit:
            shutil.move(delta_table, delta_table + "_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        else:
            last_id = pl.read_delta(delta_table)[order_by_column].max()

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

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    db_uri = "mssql://testoltp/Retail?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true"
    delta_table = "./data/urunrecete"
  
    initial_reader = read_database_part(db_uri, 
        "SELECT ID, UrunID1, UrunID2, Miktar, SonDuzenleme, VarsayilanAsorti, Hist_ID=0, Hist_Islem=0 FROM tb_UrunRecete", "ID")
    
    for df in initial_reader:
        # df = df.with_columns(
        #     pl.col("RowVersion").bin.encode("hex"),
        #     pl.lit(0, pl.Int64).alias("Hist_ID"),
        #     pl.lit(0, pl.Int16).alias("Hist_Islem")
        # )

        logger.info(f"Initial table height: {df.height}")
        df.write_delta(
            target=delta_table, 
            mode="append")

    logger.info("Initial table written")

    delta_reader = read_database_part(db_uri, 
        "SELECT ID, UrunID1, UrunID2, Miktar, SonDuzenleme, VarsayilanAsorti, Hist_ID, Hist_Islem FROM tb_UrunRecete_Hist", "Hist_ID")
    
    for df in delta_reader:
        df = df.filter(
                pl.col("Hist_ID") == pl.col("Hist_ID").max().over("ID"))
 
        logger.info(f"Delta table height: {df.height}")
        df.write_delta(
            target=delta_table, 
            mode="merge",
            delta_merge_options={
                "predicate": "s.ID = t.ID",  
                "source_alias": "s",         
                "target_alias": "t",   
            })

    logger.info("Delta table written")
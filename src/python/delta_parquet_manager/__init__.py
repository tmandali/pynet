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

def write_delta(delta_table:str, db_uri:str, query:str, order_by_column:str, reinit:bool=False, limit:int=1_000_000, partition:int=8) -> pl.DataFrame:
    if os.path.exists(delta_table) and reinit:
        shutil.move(delta_table, delta_table + "_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
  
    last_id = None
    if os.path.exists(delta_table):
       last_id = pl.read_delta(delta_table)[order_by_column].max()
    
    reader = read_database_part(db_uri, query, order_by_column, last_id, limit, partition)
    for df in reader:
        logger.info(f"Writing delta table: {delta_table} with height: {df.height}")
        df.write_delta(
            target=delta_table, 
            mode="append")

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

if __name__ == "__main__":
    logger.setLevel(logging.INFO) 

    db_uri = "mssql://testoltp/Retail?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true"
    delta_table = "./data/urunrecete"
    
    write_delta(
        delta_table, 
        db_uri, 
        """SELECT ID, UrunID1, UrunID2, Miktar, SonDuzenleme, VarsayilanAsorti, Hist_ID=0, Hist_Islem=0 
           FROM tb_UrunRecete""", 
        "ID")
    logger.info(f"Delta table write completed")
    
    write_delta_merge(
        delta_table, 
        db_uri, 
        """SELECT ID, UrunID1, UrunID2, Miktar, SonDuzenleme, VarsayilanAsorti, Hist_ID, Hist_Islem 
           FROM tb_UrunRecete_Hist""", 
        "Hist_ID", 
        "ID")
    logger.info(f"Delta table merge completed")
    
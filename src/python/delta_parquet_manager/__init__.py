import datetime
import logging
import os
import shutil
from typing import Any, Iterable
from urllib.parse import urlparse
import polars as pl
from pypika import MSSQLQuery, Order, Table
from pypika.analytics import Max, Min
from pypika.queries import QueryBuilder

logger = logging.getLogger(__name__)
if not logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def read_database_partition(db_uri:str, table_name:str, partition_on:str, columns:str|list[str]='*', last_value:Any=None, limit:int = 100_000, partition_num:int = 8) -> Iterable[tuple[Any, pl.DataFrame]]:

    table = Table(table_name)
    table_query = MSSQLQuery.from_(table).select(table[partition_on]).orderby(table[partition_on], order=Order.asc).limit(limit)

    while True:
        if last_value is not None:
            table_query = table_query.where(table[partition_on] > last_value)

        range_query = MSSQLQuery.from_(table_query).select(
            Max(table_query[partition_on]).as_('max_value'), 
            Min(table_query[partition_on]).as_('min_value'))
            
        range_sql = range_query.get_sql()
        range_df = pl.read_database_uri(range_sql, db_uri)
    
        min_value = range_df["min_value"].item()
        max_value = range_df["max_value"].item()

        part_query = MSSQLQuery.from_(table).select(*(columns if isinstance(columns, str) else columns)).where(table[partition_on] >= min_value).where(table[partition_on] <= max_value)
        part_sql = part_query.get_sql()
        part_df = pl.read_database_uri(part_sql, db_uri, partition_num=partition_num, partition_on=partition_on)

        if part_df is None or part_df.is_empty():
            break

        yield (max_value, part_df)
        last_value = max_value

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

    db_uri = "mssql://testoltp/Store7?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true"
    delta_table = "./data/urun_recete"

    initial_history_id = None
    last_value = None

    if os.path.exists(delta_table):
        df = pl.read_delta(delta_table)
        initial_history_id = df["Hist_ID"].max()
        last_value = df["ID"].max()
   
    if not initial_history_id:
        initial_history_id = pl.read_database_uri("SELECT max(Hist_ID) as initial_history_id FROM tb_Urun_Hist", db_uri)["initial_history_id"].item()

    if not initial_history_id:
        initial_history_id = 0

    for max_value, df in read_database_partition(db_uri, "tb_Urun", "ID", last_value=last_value):
        df = df.with_columns(
          pl.lit(initial_history_id).cast(pl.Int64).alias("Hist_ID"),
          pl.lit(1).cast(pl.Int16).alias("Hist_Islem"))
        df.write_delta(delta_table, mode="append")
        print(f"Max value: {max_value}, Height: {df.height}")
        print(df.head())

    for max_value, df in read_database_partition(db_uri, "tb_Urun_Hist", "Hist_ID", last_value=initial_history_id):
         df.write_delta(
            target=delta_table, 
            mode="merge",
            delta_merge_options={
                "predicate": f"s.ID = t.ID",  
                "source_alias": "s",         
                "target_alias": "t",   
            }).when_matched_update_all().when_not_matched_insert_all().execute()

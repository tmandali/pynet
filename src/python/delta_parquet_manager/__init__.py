from datetime import date
import logging
from typing import Any, Iterable
import polars as pl

class DeltaParquetManager:
    def __init__(self): 
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            # Console Handler
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def _generate_range(self, min_id:int, max_id:int, partition:int) -> Iterable[tuple[int, int]]:
        bucket_size = int((max_id - min_id) / partition)
        for i in range(partition):
            start_id = min_id + i * bucket_size
            end_id = max_id if i == partition - 1 else start_id + bucket_size - 1
            yield start_id, end_id

    def _read_data_part(self, last_id:Any, db_uri:str, query:str, order_by_column:str, limit:int, partition:int) -> pl.DataFrame:      

        range_query = f"""           
            WITH Part AS (SELECT TOP {limit} {order_by_column} FROM ({query}) source WHERE {order_by_column} > '{last_id}' ORDER BY 1),
            Range AS (SELECT NTILE({partition}) OVER (ORDER BY {order_by_column}) as Part, {order_by_column} FROM Part)
            SELECT Part, MIN({order_by_column}) as BeginPart, MAX({order_by_column}) as EndPart FROM Range GROUP BY Part
        """
        print(range_query)

        range = pl.read_database_uri(range_query, db_uri).to_dicts()
        if range is None or len(range) == 0:
            return None

        part_list: list[str] = [f"""
            SELECT * FROM ({query}) part WHERE {order_by_column} BETWEEN '{range["BeginPart"]}' AND '{range["EndPart"]}'
            """ for range in range]

        # min_id = range["min_id"]
        # max_id = range["max_id"]        

        # part_list: list[str] = [f"""
        #     SELECT * FROM ({query}) source where {order_by_column} between '{start_id}' and '{end_id}'
        #     """ for start_id, end_id in self._generate_range(min_id, max_id, partition)]
                
        df = pl.read_database_uri(part_list, db_uri)
        if df.is_empty():
            return None

        return df                
    
    def read_database_part(self, db_uri:str, query:str, order_by_column:str, last_id:Any = 0, limit:int=1_000_000, partition:int=8):     
        if partition <= 0 or partition is None:
            partition = 1
        
        while True:
            df = self._read_data_part(last_id, db_uri, query, order_by_column, limit, partition)
            if df is None:
                break

            last_id = df[order_by_column].max()
            yield df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db_reader = DeltaParquetManager().read_database_part(
        db_uri="mssql://testoltp/Store7?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true", 
        query="SELECT * FROM tb_UrunRecete", 
        order_by_column="ID")

    for df in db_reader:
        print(df.height)
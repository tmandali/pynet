import logging
from typing import Iterable
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

    def read_database_part(self, db_uri:str, out_dir:str, query:str, order_by_column:str, limit:int=1_000_000, partition:int=8):     
        if partition <= 0 or partition is None:
            partition = 1
        
        last_id = 0
        while True:
            df = self._read_data_part(last_id, db_uri, query, order_by_column, limit, partition)
            if df is None:
                break

            last_id = df[order_by_column].max()
            yield df

    def _generate_range(self, min_id:int, max_id:int, partition:int) -> Iterable[tuple[int, int]]:
        bucket_size = int((max_id - min_id) / partition)
        for i in range(partition):
            start_id = min_id + i * bucket_size
            end_id = max_id if i == partition - 1 else start_id + bucket_size - 1
            yield start_id, end_id

    def _read_data_part(self, last_id:int, db_uri:str, query:str, order_by_column:str, limit:int, partition:int) -> pl.DataFrame:        
        range_query = f"""           
            WITH Part AS (SELECT TOP {limit} {order_by_column} FROM ({query}) source WHERE {order_by_column} > {last_id} ORDER BY 1)
            SELECT MIN({order_by_column}) as min_id, MAX({order_by_column}) as max_id FROM Part
        """
        self.logger.info(f"range_query: {range_query}")

        df = pl.read_database_uri(range_query, db_uri)
        if df.is_empty():
            return None

        min_id = df["min_id"][0]
        max_id = df["max_id"][0]        

        part_list: list[str] = [f"""
            SELECT * FROM ({query}) source where {order_by_column} between {start_id} and {end_id}
            """ for start_id, end_id in self._generate_range(min_id, max_id, partition)]
                
        df = pl.read_database_uri(part_list, db_uri)
        if df.is_empty():
            return None

        return df                
            
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = DeltaParquetManager()
    db_reader = manager.read_database_part(
        db_uri="mssql://testoltp/Store7?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true", 
        out_dir="urunrecete", 
        query="SELECT * FROM tb_UrunRecete", 
        order_by_column="ID")

    for df in db_reader:
        print(df.describe())
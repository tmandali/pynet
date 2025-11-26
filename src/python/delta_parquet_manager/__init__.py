import logging
from typing import Iterable
import polars as pl

class DeltaParquetManager:
    def __init__(self, db_uri:str, out_dir:str, query:str, pk_columns:str|list[str], order_by_column:str, limit:int=1_000_000, partition:int=8):
        self.db_uri = db_uri
        self.out_dir = out_dir
        self.limit = limit
        self.pk_columns = pk_columns
        self.order_by_column = order_by_column
        self.query = query

        self.partition = 1        
        if partition > 0:
            self.partition = partition

        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            # Console Handler
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def run(self):        
        last_id = 0
        while True:
            df = self._read_data_part(last_id)
            if df is None:
                break

            last_id = df[self.order_by_column].max()
            yield df

    def _generate_range(self, min_id:int, max_id:int, partition:int) -> Iterable[tuple[int, int]]:
        bucket_size = int((max_id - min_id) / partition)
        for i in range(partition):
            start_id = min_id + i * bucket_size
            end_id = max_id if i == partition - 1 else start_id + bucket_size - 1
            yield start_id, end_id

    def _read_data_part(self, last_id:int) -> pl.DataFrame:        
        range_query = f"""           
            WITH Part AS (SELECT TOP {self.limit} {self.order_by_column} FROM ({self.query}) source WHERE {self.order_by_column} > {last_id} ORDER BY 1)
            SELECT MIN({self.order_by_column}) as min_id, MAX({self.order_by_column}) as max_id FROM Part
        """
        self.logger.info(f"range_query: {range_query}")

        df = pl.read_database_uri(range_query, self.db_uri)
        if df.is_empty():
            return None

        min_id = df["min_id"][0]
        max_id = df["max_id"][0]        

        part_list: list[str] = [f"""
            SELECT * FROM ({self.query}) source where {self.order_by_column} between {start_id} and {end_id}
            """ for start_id, end_id in self._generate_range(min_id, max_id, self.partition)]
        
        self.logger.info(f"part_list: {part_list}")
        
        df = pl.read_database_uri(part_list, self.db_uri)
        if df.is_empty():
            return None

        return df                
            
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = DeltaParquetManager(
        db_uri="mssql://testoltp/Store7?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true",
        out_dir="veri_ambari",
        query="SELECT * FROM tb_UrunRecete",
        pk_columns="ID",
        order_by_column="ID",
        limit=100_000
    )

    for df in manager.run():
        print(df.describe())
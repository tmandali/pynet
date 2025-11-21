import os
from typing import Optional

#try:
#    import pyarrow as pa
#    HAS_ARROW = True
#except ImportError:
#    pa = None
#    HAS_ARROW = False

#try:
#    import polars as pl
#    HAS_POLARS = True
#except ImportError:
#    pl = None
#    HAS_POLARS = False

#try:
#    import pandas as pd
#    HAS_PANDAS = True
#except ImportError:
#    pd = None
#    HAS_PANDAS = False

import connectorx as cx
import polars as pl
import pyarrow as pa

class Sync:
    
    def __init__(
        self,
        
        src_conn: Optional[str] = None,
        src_stream: Optional[str] = None,
        
        tgt_conn: Optional[str] = None,
        tgt_object: Optional[str] = None,

        input: Optional[pl.DataFrame] = None,
    ):
        """
        Initialize Sync

        Args:
            src_conn: Source database/storage connection
            src_stream: Source table, file path, or SQL query

            tgt_conn: Target database connection
            tgt_object: Target table or file path

            input: Input data - can be a Python iterable (list of dicts), pandas DataFrame, or polars DataFrame
        """

        self.src_conn = src_conn
        self.src_stream = src_stream

        self.tgt_conn = tgt_conn
        self.tgt_object = tgt_object

        self.input = input

    def run(self):
        
        # Prepare environment
        env = dict(os.environ)

        if not self.input:
            if self.src_conn and self.src_conn in env:
                self.src_conn = env[self.src_conn]

            self.input = cx.read_sql(self.src_conn, self.src_stream, return_type="polars")
            
            if not self.src_conn:
                raise ValueError("src_conn is required when input is not provided")
            
            if not self.src_stream:
                raise ValueError("src_stream is required: Source table, file path, or SQL query")

        if not self.tgt_object:
            raise ValueError("tgt_object is required: Target table or file path")

        if self.tgt_conn and self.tgt_conn in env:
            self.tgt_conn = env[self.tgt_conn]
                    
        if self.tgt_conn:
            self.input.write_database(self.tgt_conn, self.tgt_object)
            return
        
        # Write to parquet
        self.input.write_parquet(
            self.tgt_object, 
            compression="zstd", 
            use_pyarrow=True, 
            pyarrow_options={"row_group_size": 100_000}
        )
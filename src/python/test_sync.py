from typing import Any, Dict, Iterable, Iterator
import os
import duckdb
from pyarrow.ipc import read_record_batch
from sync import Sync
import connectorx as cx
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

conn_str = "mssql://testoltp/Store7?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true"
query = """
   SELECT *, ID / 1000000 as Chunk 
   FROM tb_UrunRecete (nolock) 
   order by ID
   """

def read_batches_arrow(reader: pa.ipc.RecordBatchStreamReader) -> Iterator[pa.Table]:     
    for batch in reader:
        yield pa.Table.from_batches([batch])

    #  for batch in reader:       
    #     table = pa.table([batch[i] for i in range(batch.num_columns)], 
    #                 names=batch.schema.names)

def read_batches_dict(reader: pa.ipc.RecordBatchStreamReader) -> Iterator[Dict[str, Any]]:                    # Read batches and yield records
    for table in read_batches_arrow(reader):       
        for row in table.to_pylist():
            yield row

    # Convert to Python objects with type preservation
    # for row_idx in range(table.num_rows):
    #     record = {}
    #     for col_idx, column_name in enumerate(table.column_names):
    #         column = table.column(col_idx)
    #         value = column[row_idx].as_py()  # Converts to native Python type
    #         record[column_name] = value
        
    #     yield record


reader: pa.ipc.RecordBatchStreamReader = cx.read_sql(conn_str, query, return_type="arrow_stream", batch_size=100000)

ds.write_dataset(
    reader, 
    base_dir="dataset_output", 
    format="parquet", 
    partitioning=["Chunk"]
)

# os.makedirs("batch_output", exist_ok=True)
# for i, batch in enumerate(reader):
#     table = pa.Table.from_batches([batch])
#     output_file = f"batch_output/batch_{i}.parquet"
#     pq.write_table(table, output_file)
#     print(f"Batch {i} yazıldı: {output_file} ({batch.num_rows} satır)")


# Reader 'write_dataset' tarafından tüketildiği için aşağıdaki kısım çalışmayacaktır (stream bitmiştir).
# Eğer hem parquet hem arrow dosyası istiyorsanız, sorguyu iki kere çalıştırmanız gerekir.
# with pa.OSFile('bigfile.arrow', 'wb') as sink:
#    with pa.ipc.new_file(sink, reader.schema) as writer:
#       for batch in reader:
#         writer.write(batch)

#with pa.OSFile('bigfile.arrow', 'rb') as source:
#   loaded_array = pa.ipc.open_file(source).read_all()

#print("LEN:", len(loaded_array))
#print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))

#for batch in reader:
#    # Process each batch separately to manage memory
#    print(f"Processing batch with {batch.num_rows} rows")
#    # Convert batch to pandas if needed
#    batch_df = batch.to_pandas()

#table = reader.read_all()
#print(f"Received {table.num_rows} rows with {table.num_columns} columns")
#print(f"Schema: {table.schema}")

#print(f"Column names: {table.column_names}")
#for column_name in table.column_names:
#    column = table.column(column_name)
#    print(f"{column_name}: {column.type}")
from typing import Any, Dict, Iterable, Iterator
import duckdb
from pyarrow.ipc import read_record_batch
from sync import Sync
import connectorx as cx
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

conn_str = "mssql://testoltp/Store7?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true"
# Tarih kolonunuzun adını buraya yazın (Örn: Tarih, OlusturmaTarihi vb.)
tarih_kolonu = "Tarih" 
query = f"SELECT top 1000000 *, YEAR({tarih_kolonu}) as Yil FROM tb_UrunRecete"

def read_batches(reader: pa.ipc.RecordBatchStreamReader) -> Iterator[Dict[str, Any]]:                    # Read batches and yield records
    for batch in reader:       
        table = pa.table([batch[i] for i in range(batch.num_columns)], 
                    names=batch.schema.names)
    
    # Convert to Python objects with type preservation
    for row_idx in range(table.num_rows):
        record = {}
        for col_idx, column_name in enumerate(table.column_names):
            column = table.column(col_idx)
            value = column[row_idx].as_py()  # Converts to native Python type
            record[column_name] = value
        
        yield record

reader: pa.ipc.RecordBatchStreamReader = cx.read_sql(conn_str, query, return_type="arrow_stream", batch_size=100000)

ds.write_dataset(
    reader, 
    base_dir="dataset_output", 
    format="parquet", 
    partitioning=["Yil"], 
    schema=reader.schema,
    existing_data_behavior="overwrite_or_ignore"
)


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
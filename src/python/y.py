from deltalake import DeltaTable, write_deltalake
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl

# for df in pl.scan_parquet("urun.parquet").collect_batches():
#     df.write_delta("data_files/main/urun", mode="append")

parquet_file = pq.ParquetFile("urun.parquet")
print(parquet_file.schema_arrow)
for batch in parquet_file.iter_batches(batch_size=100_000):
    write_deltalake(
        "data_files/main/urun", 
        batch, 
        mode="append")

#configuration={'delta.enableDeletionVectors': 'true'}

# dt.merge(
#     source=pl.DataFrame({"id": [1, 2, 3]}).to_arrow(),
#     predicate="t.id = s.id",
#     source_alias="s",
#     target_alias="t"
# ).when_matched_update_all().when_not_matched_insert_all().execute()
import os
import tempfile
import time
import shutil
from typing import Iterator, Union, Optional

import polars as pl
import pyarrow.parquet as pq
from sqlalchemy import create_engine, Engine

from .config import TableConfig


class DatabaseToParquet:
    """Veritabanından Parquet dosyasına veri aktarımı için sınıf
    
    MSSQL, PostgreSQL ve diğer SQLAlchemy destekli veritabanları ile çalışır.
    Local dosya sistemi ve S3 desteği vardır.
    """
    
    def __init__(
        self,
        connection: Union[str, Engine],
        table_config: TableConfig,
    ):
        """
        Args:
            connection: SQLAlchemy connection string veya Engine objesi
                - MSSQL: "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
                - PostgreSQL: "postgresql://user:pass@host:5432/db"
                - SQLite: "sqlite:///path/to/db.sqlite"
            table_config: Tablo konfigürasyonu (batch_size config içinde)
                - S3 için: output_file="s3://bucket/path/file.parquet"
        """
        self.config = table_config
        self._fs = None  # Lazy initialization for S3 filesystem
        
        if isinstance(connection, str):
            self.engine = create_engine(connection)
            self._owns_engine = True
        else:
            self.engine = connection
            self._owns_engine = False
    
    def _get_s3_fs(self):
        """S3 filesystem objesini döndürür (lazy initialization)"""
        if self._fs is None and self.config.is_s3_path:
            try:
                import s3fs
                storage_opts = self.config.storage_options or {}
                self._fs = s3fs.S3FileSystem(**storage_opts)
            except ImportError:
                raise ImportError("S3 desteği için s3fs kurulu olmalı: pip install s3fs")
        return self._fs
    
    def _file_exists(self, path: str) -> bool:
        """Dosyanın var olup olmadığını kontrol eder (local veya S3)"""
        if path.startswith("s3://"):
            fs = self._get_s3_fs()
            return fs.exists(path.replace("s3://", ""))
        return os.path.exists(path)
    
    def _upload_to_s3(self, local_path: str, s3_path: str) -> None:
        """Local dosyayı S3'e yükler"""
        fs = self._get_s3_fs()
        s3_key = s3_path.replace("s3://", "")
        fs.put(local_path, s3_key)
        print(f"[{self.config.table_name}] S3'e yüklendi: {s3_path}")
    
    def _copy_s3(self, src: str, dst: str) -> None:
        """S3'te dosya kopyalar"""
        fs = self._get_s3_fs()
        src_key = src.replace("s3://", "")
        dst_key = dst.replace("s3://", "")
        fs.copy(src_key, dst_key)
    
    def _delete_s3(self, path: str) -> None:
        """S3'ten dosya siler"""
        fs = self._get_s3_fs()
        s3_key = path.replace("s3://", "")
        if fs.exists(s3_key):
            fs.rm(s3_key)
    
    def _finalize_output(self, temp_path: str, backup: bool = True) -> None:
        """Temp dosyayı hedef konuma taşır (local veya S3)"""
        output = self.config.output_file
        
        if self.config.is_s3_path:
            # S3 için
            if backup and self._file_exists(output):
                backup_path = f"{output}.backup"
                self._copy_s3(output, backup_path)
                print(f"[{self.config.table_name}] S3 yedek: {backup_path}")
            self._upload_to_s3(temp_path, output)
        else:
            # Local için
            if backup and os.path.exists(output):
                backup_path = f"{output}.backup"
                os.replace(output, backup_path)
                print(f"[{self.config.table_name}] Yedek dosya: {backup_path}")
            os.replace(temp_path, output)
    
    def _get_column_list(self, include_hist: bool = False) -> str:
        """Sütun listesini string olarak döndürür"""
        cols = self.config.columns.copy()
        if include_hist:
            cols.extend([self.config.hist_id_column, self.config.hist_operation_column])
        return ', '.join(cols)
    
    def _read_changes(self, hist_id: int) -> Iterator[pl.DataFrame]:
        """Belirtilen Hist_ID'den sonraki değişiklikleri okur"""
        query = f"""
            SELECT {self._get_column_list(include_hist=True)}
            FROM {self.config.hist_table_name}
            WHERE {self.config.hist_id_column} > :hist_id
            ORDER BY {self.config.hist_id_column}
        """
        
        for df in pl.read_database(
            query=query,
            connection=self.engine.execution_options(stream_results=True),
            iter_batches=True,
            batch_size=self.config.batch_size,
            execute_options={"parameters": {"hist_id": hist_id}}
        ):
            yield df
    
    def init_parquet(self) -> None:
        """Ana tablodan ilk Parquet dosyasını oluşturur (temp file üzerinden)"""
        start_time = time.time()
        row_count = 0
        temp_file_path = None
        
        query = f"""
            DECLARE @{self.config.hist_id_column} BIGINT
            SELECT @{self.config.hist_id_column} = MAX({self.config.hist_id_column}) 
            FROM {self.config.hist_table_name}
            
            SELECT {self._get_column_list()}, 
                   @{self.config.hist_id_column} as {self.config.hist_id_column}, 
                   {self.config.hist_operation_column} = 1
            FROM {self.config.table_name}
        """
        
        try:
            with tempfile.NamedTemporaryFile(mode='w+b', suffix='.parquet', delete=False) as temp_file:
                temp_file_path = temp_file.name
                writer = None
                
                for i, df in enumerate(pl.read_database(
                    query=query,
                    connection=self.engine.execution_options(stream_results=True),
                    iter_batches=True,
                    batch_size=self.config.batch_size
                )):
                    table = df.to_arrow()
                    
                    if writer is None:
                        writer = pq.ParquetWriter(temp_file, table.schema)
                    
                    writer.write_table(table)
                    row_count += len(df)
                    print(f"[{self.config.table_name}] Parça {i+1} işlendi. Toplam: {row_count:,} satır.")
                
                if writer:
                    writer.close()
            
            # Başarılı ise dosyayı hedef konuma taşı (local veya S3)
            if row_count > 0:
                self._finalize_output(temp_file_path, backup=True)
                temp_file_path = None  # Başarılı, silme
                
                elapsed = (time.time() - start_time) / 60
                print(f"[{self.config.table_name}] İlk yükleme tamamlandı! Süre: {elapsed:.2f} dk")
            else:
                print(f"[{self.config.table_name}] Veri bulunamadı!")
        
        except Exception as e:
            print(f"\n!!! HATA !!!: {e}")
            raise
        
        finally:
            # Hata durumunda temp dosyayı temizle
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
    
    def _scan_parquet(self, path: str) -> pl.LazyFrame:
        """Parquet dosyasını scan eder (local veya S3)"""
        if path.startswith("s3://"):
            return pl.scan_parquet(path, storage_options=self.config.storage_options)
        return pl.scan_parquet(path)
    
    def sync_changes(self) -> bool:
        """Değişiklikleri senkronize eder. Değişiklik varsa True döner."""
        start_time = time.time()
        
        # Mevcut en yüksek Hist_ID'yi al
        current_hist_id = (
            self._scan_parquet(self.config.output_file)
            .select(pl.col(self.config.hist_id_column))
            .max()
            .collect()
            .item()
        )
        print(f"[{self.config.table_name}] Mevcut {self.config.hist_id_column}: {current_hist_id}")
        
        writer = None
        has_changes = False
        
        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.parquet', delete=False) as change_file:
            change_file_path = change_file.name
            
            # Değişiklikleri geçici dosyaya yaz
            for change_df in self._read_changes(current_hist_id):
                has_changes = True
                table = change_df.to_arrow()
                
                if writer is None:
                    writer = pq.ParquetWriter(change_file, schema=table.schema)
                
                writer.write_table(table)
            
            if writer:
                writer.close()
        
        if not has_changes:
            print(f"[{self.config.table_name}] Değişiklik bulunamadı.")
            os.unlink(change_file_path)
            return False
        
        output_file_path = None
        try:
            # Değişiklikleri işle
            change_df = (
                pl.scan_parquet(change_file_path)
                .sort(self.config.hist_id_column)
                .unique(subset=[self.config.primary_key], keep="last")
            )
            
            parquet_writer = None
            with tempfile.NamedTemporaryFile(mode='w+b', suffix='.parquet', delete=False) as output_file:
                output_file_path = output_file.name
                
                # Aktif kayıtları yaz (Hist_Islem > 0)
                for batch in change_df.filter(
                    pl.col(self.config.hist_operation_column) > 0
                ).collect_batches(chunk_size=self.config.batch_size):
                    table = batch.to_arrow()
                    if parquet_writer is None:
                        parquet_writer = pq.ParquetWriter(output_file, schema=table.schema)
                    parquet_writer.write_table(table)
                
                if parquet_writer:
                    # Mevcut dosyadan değişmeyen kayıtları ekle
                    for batch in (
                        self._scan_parquet(self.config.output_file)
                        .join(change_df, on=self.config.primary_key, how="anti")
                        .collect_batches(chunk_size=self.config.batch_size)
                    ):
                        parquet_writer.write(batch.to_arrow())
                    
                    parquet_writer.close()
            
            # Dosyaları hedef konuma taşı (local veya S3)
            if parquet_writer:
                self._finalize_output(output_file_path, backup=True)
                output_file_path = None  # Başarılı
            
        finally:
            # Geçici dosyaları temizle
            if os.path.exists(change_file_path):
                os.unlink(change_file_path)
            if output_file_path and os.path.exists(output_file_path):
                os.unlink(output_file_path)
        
        elapsed = (time.time() - start_time) / 60
        print(f"[{self.config.table_name}] Senkronizasyon tamamlandı! Süre: {elapsed:.2f} dk")
        return True
    
    def run(self, force_init: bool = False) -> None:
        """Ana çalıştırma metodu. Dosya yoksa init, varsa sync yapar."""
        if force_init or not self._file_exists(self.config.output_file):
            print(f"[{self.config.table_name}] İlk yükleme başlatılıyor...")
            self.init_parquet()
        else:
            print(f"[{self.config.table_name}] Değişiklikler senkronize ediliyor...")
            self.sync_changes()
    
    def dispose(self) -> None:
        """Kaynakları temizler (sadece engine'i biz oluşturduysak)"""
        if self._owns_engine:
            self.engine.dispose()
    
    def __enter__(self):
        """Context manager desteği için"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager çıkışında otomatik dispose"""
        self.dispose()
        return False


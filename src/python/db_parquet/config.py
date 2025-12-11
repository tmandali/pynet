from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class S3Config:
    """S3 konfigürasyonu"""
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    endpoint_url: Optional[str] = None  # MinIO, LocalStack vb. için
    region: Optional[str] = None
    
    def to_storage_options(self) -> Dict[str, Any]:
        """fsspec/s3fs için storage_options döndürür"""
        options = {}
        if self.access_key:
            options["key"] = self.access_key
        if self.secret_key:
            options["secret"] = self.secret_key
        if self.endpoint_url:
            options["endpoint_url"] = self.endpoint_url
            options["client_kwargs"] = {"endpoint_url": self.endpoint_url}
        if self.region:
            options["region_name"] = self.region
        return options if options else None


@dataclass
class TableConfig:
    """Tablo konfigürasyonu için veri sınıfı"""
    table_name: str
    columns: List[str]
    primary_key: str = "ID"
    hist_table_suffix: str = "_Hist"
    hist_id_column: str = "Hist_ID"
    hist_operation_column: str = "Hist_Islem"
    output_file: Optional[str] = None
    batch_size: int = 100_000
    s3_config: Optional[S3Config] = None
    
    def __post_init__(self):
        if self.output_file is None:
            # tb_Urun -> urun.parquet
            self.output_file = f"{self.table_name.lower().replace('tb_', '')}.parquet"
    
    @property
    def hist_table_name(self) -> str:
        """History tablo adını döndürür"""
        return f"{self.table_name}{self.hist_table_suffix}"
    
    @property
    def is_s3_path(self) -> bool:
        """output_file bir S3 path mi kontrol eder"""
        return self.output_file and self.output_file.startswith("s3://")
    
    @property
    def storage_options(self) -> Optional[Dict[str, Any]]:
        """S3 için storage options döndürür"""
        if self.s3_config:
            return self.s3_config.to_storage_options()
        return None


# db_parquet

Veritabanından Parquet dosyasına veri aktarımı için Python modülü.

## Özellikler

- ✅ **Çoklu Veritabanı Desteği**: MSSQL, PostgreSQL, MySQL, SQLite
- ✅ **S3 Desteği**: AWS S3, MinIO, LocalStack
- ✅ **Incremental Sync**: History tablosu üzerinden değişiklik takibi
- ✅ **Güvenli Yazma**: Temp file kullanarak atomik işlemler
- ✅ **Otomatik Backup**: Mevcut dosya `.backup` olarak yedeklenir
- ✅ **Batch İşleme**: Büyük tablolar için bellek verimli aktarım

---

## Kurulum

```bash
# Temel bağımlılıklar
pip install polars pyarrow sqlalchemy

# MSSQL için
pip install pyodbc
# veya
pip install pymssql

# PostgreSQL için
pip install psycopg2-binary

# S3 desteği için
pip install s3fs
```

---

## Hızlı Başlangıç

```python
from db_parquet import TableConfig, DatabaseToParquet, get_mssql_connection

# Tablo konfigürasyonu
config = TableConfig(
    table_name="tb_Urun",
    columns=['ID', 'UrunKod', 'Renk', 'Beden'],
    primary_key="ID",
    output_file="urun.parquet"
)

# Çalıştır
with DatabaseToParquet(
    connection=get_mssql_connection('SERVER', 'DATABASE'),
    table_config=config
) as converter:
    converter.run()
```

---

## TableConfig Parametreleri

| Parametre | Tip | Varsayılan | Açıklama |
|-----------|-----|------------|----------|
| `table_name` | str | **Zorunlu** | Ana tablo adı (örn: `tb_Urun`) |
| `columns` | List[str] | **Zorunlu** | Çekilecek sütunlar |
| `primary_key` | str | `"ID"` | Birincil anahtar sütunu |
| `hist_table_suffix` | str | `"_Hist"` | History tablo eki |
| `hist_id_column` | str | `"Hist_ID"` | History ID sütunu |
| `hist_operation_column` | str | `"Hist_Islem"` | İşlem tipi sütunu (1=insert/update, 0=delete) |
| `output_file` | str | `{table}.parquet` | Çıktı dosya yolu veya S3 URI |
| `batch_size` | int | `100_000` | Batch boyutu |
| `s3_config` | S3Config | `None` | S3 konfigürasyonu |

---

## Connection String Örnekleri

### MSSQL

```python
from db_parquet import get_mssql_connection, get_mssql_pyodbc_simple

# Windows Authentication (ODBC)
conn = get_mssql_connection('SERVER', 'DATABASE')

# SQL Authentication (ODBC) - basit format
conn = get_mssql_pyodbc_simple('host', 'db', 'user', 'pass', port=1433)

# SQL Authentication (pymssql) - pip install pymssql gerekir
from db_parquet import get_mssql_simple
conn = get_mssql_simple('host', 'db', 'user', 'pass', port=1433)
```

### PostgreSQL

```python
from db_parquet import get_postgres_connection

conn = get_postgres_connection('localhost', 'mydb', 'user', 'pass', port=5432)
# Sonuç: postgresql://user:pass@localhost:5432/mydb
```

### MySQL

```python
from db_parquet import get_mysql_connection

conn = get_mysql_connection('localhost', 'mydb', 'user', 'pass', port=3306)
# Sonuç: mysql+pymysql://user:pass@localhost:3306/mydb
```

### SQLite

```python
from db_parquet import get_sqlite_connection

conn = get_sqlite_connection('path/to/database.db')
# Sonuç: sqlite:///path/to/database.db
```

---

## S3 Desteği

### AWS S3

```python
from db_parquet import TableConfig, S3Config, DatabaseToParquet

s3_config = S3Config(
    access_key="AKIAXXXXXXXX",
    secret_key="your-secret-key",
    region="eu-west-1"
)

config = TableConfig(
    table_name="tb_Urun",
    columns=['ID', 'UrunKod'],
    output_file="s3://my-bucket/data/urun.parquet",
    s3_config=s3_config
)
```

### MinIO / LocalStack

```python
s3_config = S3Config(
    access_key="minioadmin",
    secret_key="minioadmin",
    endpoint_url="http://localhost:9000"
)

config = TableConfig(
    table_name="tb_Urun",
    columns=['ID', 'UrunKod'],
    output_file="s3://my-bucket/urun.parquet",
    s3_config=s3_config
)
```

### S3Config Parametreleri

| Parametre | Tip | Açıklama |
|-----------|-----|----------|
| `access_key` | str | AWS Access Key ID |
| `secret_key` | str | AWS Secret Access Key |
| `endpoint_url` | str | Custom endpoint (MinIO, LocalStack vb.) |
| `region` | str | AWS Region (örn: `eu-west-1`) |

---

## Metodlar

### `run(force_init=False)`

Ana çalıştırma metodu. Dosya yoksa `init_parquet`, varsa `sync_changes` çağırır.

```python
converter.run()           # Otomatik karar
converter.run(force_init=True)  # Her zaman sıfırdan oluştur
```

### `init_parquet()`

Ana tablodan ilk parquet dosyasını oluşturur.

- Temp file üzerinde çalışır
- Başarılı olursa mevcut dosyayı yedekler
- Atomik replace işlemi yapar

### `sync_changes()`

History tablosundan değişiklikleri senkronize eder.

- Mevcut `Hist_ID`'den sonraki kayıtları alır
- Insert/Update (`Hist_Islem > 0`) kayıtlarını ekler
- Delete (`Hist_Islem = 0`) kayıtlarını çıkarır
- Atomik replace işlemi yapar

---

## Veritabanı Gereksinimi

Bu modül, tablonuzun bir **History tablosu** olduğunu varsayar:

```sql
-- Ana tablo
CREATE TABLE tb_Urun (
    ID BIGINT PRIMARY KEY,
    UrunKod NVARCHAR(50),
    Renk NVARCHAR(50),
    -- diğer sütunlar...
)

-- History tablosu
CREATE TABLE tb_Urun_Hist (
    Hist_ID BIGINT IDENTITY PRIMARY KEY,
    Hist_Islem INT,  -- 1=Insert/Update, 0=Delete
    ID BIGINT,
    UrunKod NVARCHAR(50),
    Renk NVARCHAR(50),
    -- diğer sütunlar...
)
```

---

## Tam Örnek

```python
from db_parquet import (
    TableConfig,
    DatabaseToParquet,
    get_mssql_connection
)

# Konfigürasyon
urun_config = TableConfig(
    table_name="tb_Urun",
    columns=[
        'ID', 'UrunID', 'UrunKod', 'Renk', 'Beden', 'Boy', 
        'Tarih', 'Line', 'NumuneVaryantSira'
    ],
    primary_key="ID",
    output_file="urun.parquet",
    batch_size=100_000
)

# Connection
connection = get_mssql_connection('MYSERVER', 'MyDatabase')

# Çalıştır
with DatabaseToParquet(connection, urun_config) as converter:
    # İlk çalıştırma: init_parquet
    # Sonraki çalıştırmalar: sync_changes
    converter.run()
```

---

## Dosya Yapısı

```
db_parquet/
├── __init__.py       # Export'lar
├── config.py         # TableConfig, S3Config
├── converter.py      # DatabaseToParquet
├── connections.py    # Connection helper fonksiyonları
└── README.md         # Bu dosya
```

---

## Notlar

1. **Güvenlik**: Temp file kullanıldığı için işlem sırasında hata olursa orijinal dosya bozulmaz.

2. **Backup**: Her başarılı işlemde mevcut dosya `.backup` uzantısıyla yedeklenir.

3. **S3**: S3'e yazarken önce local temp file oluşturulur, sonra upload edilir.

4. **Batch**: Büyük tablolar için `batch_size` parametresi bellek kullanımını kontrol eder.

5. **History Tablosu**: `Hist_Islem` sütunu:
   - `> 0`: Insert veya Update
   - `= 0`: Delete (kayıt parquet'ten çıkarılır)


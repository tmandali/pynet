import duckdb
import polars as pl
import os
from pathlib import Path

def read_urunrecete_from_parquet(veri_ambari_dir: str = "veri_ambari", table_name: str = "tb_UrunRecete") -> pl.DataFrame:
    """
    DuckDB kullanarak veri_ambari klasöründeki tb_UrunRecete parquet dosyalarını okur.
    
    Args:
        veri_ambari_dir: Parquet dosyalarının bulunduğu klasör yolu
        table_name: Tablo adı (klasör adı)
    
    Returns:
        Polars DataFrame
    """
    # Tablo klasörünün tam yolu
    table_dir = os.path.join(veri_ambari_dir, table_name)
    
    if not os.path.exists(table_dir):
        raise FileNotFoundError(f"Klasör bulunamadı: {table_dir}")
    
    # DuckDB bağlantısı oluştur
    con = duckdb.connect()
    
    # Parquet dosyalarını okumak için wildcard kullan
    # DuckDB, klasördeki tüm parquet dosyalarını otomatik olarak birleştirir
    parquet_pattern = os.path.join(table_dir, "*.parquet")
    
    # SQL sorgusu ile parquet dosyalarını oku
    query = f"SELECT * FROM read_parquet('{parquet_pattern}')"
    
    # DuckDB sorgusunu çalıştır ve Polars DataFrame'e dönüştür
    df = con.execute(query).pl()
    
    con.close()
    
    return df


def read_urunrecete_with_filter(veri_ambari_dir: str = "veri_ambari", 
                                 table_name: str = "tb_UrunRecete",
                                 where_clause: str = None) -> pl.DataFrame:
    """
    DuckDB kullanarak parquet dosyalarını filtreleyerek okur.
    
    Args:
        veri_ambari_dir: Parquet dosyalarının bulunduğu klasör yolu
        table_name: Tablo adı
        where_clause: SQL WHERE koşulu (örn: "ID > 1000 AND ID < 2000")
    
    Returns:
        Polars DataFrame
    """
    table_dir = os.path.join(veri_ambari_dir, table_name)
    parquet_pattern = os.path.join(table_dir, "*.parquet")
    
    con = duckdb.connect()
    
    # WHERE koşulu varsa ekle
    if where_clause:
        query = f"SELECT * FROM read_parquet('{parquet_pattern}') WHERE {where_clause}"
    else:
        query = f"SELECT * FROM read_parquet('{parquet_pattern}')"
    
    df = con.execute(query).pl()
    con.close()
    
    return df


def read_urunrecete_register_method(veri_ambari_dir: str = "veri_ambari",
                                     table_name: str = "tb_UrunRecete") -> pl.DataFrame:
    """
    Alternatif yöntem: Polars ile okuyup DuckDB'ye register ederek sorgulama.
    Bu yöntem daha fazla kontrol sağlar.
    """
    table_dir = os.path.join(veri_ambari_dir, table_name)
    
    # Polars ile tüm parquet dosyalarını oku
    parquet_files = list(Path(table_dir).glob("*.parquet"))
    
    if not parquet_files:
        raise FileNotFoundError(f"Parquet dosyası bulunamadı: {table_dir}")
    
    # Tüm parquet dosyalarını birleştir
    dfs = [pl.read_parquet(str(f)) for f in parquet_files]
    df_combined = pl.concat(dfs)
    
    # DuckDB'ye register et
    con = duckdb.connect()
    con.register("urunrecete", df_combined)
    
    # SQL sorgusu çalıştır
    df_result = con.execute("SELECT * FROM urunrecete").pl()
    
    con.close()
    
    return df_result


if __name__ == "__main__":
    # Örnek 1: Tüm veriyi oku
    print("📖 Tüm tb_UrunRecete verileri okunuyor...")
    df = read_urunrecete_from_parquet()
    print(f"✅ {len(df)} satır okundu")
    print(f"📊 Kolonlar: {df.columns}")
    print(f"\nİlk 5 satır:")
    print(df.head())
    
    # Örnek 2: Filtreli okuma
    print("\n" + "="*50)
    print("📖 Filtreli veri okunuyor (ID > 0)...")
    df_filtered = read_urunrecete_with_filter(where_clause="ID > 0 LIMIT 10")
    print(f"✅ {len(df_filtered)} satır okundu")
    print(df_filtered)
    
    # Örnek 3: Register yöntemi ile
    print("\n" + "="*50)
    print("📖 Register yöntemi ile okunuyor...")
    df_registered = read_urunrecete_register_method()
    print(f"✅ {len(df_registered)} satır okundu")
    
    # Örnek 4: DuckDB ile gelişmiş sorgular
    print("\n" + "="*50)
    print("📖 DuckDB ile gelişmiş sorgu örneği...")
    con = duckdb.connect()
    table_dir = os.path.join("veri_ambari", "tb_UrunRecete")
    parquet_pattern = os.path.join(table_dir, "*.parquet")
    
    # Örnek: Toplam kayıt sayısı, min/max ID
    query = f"""
    SELECT 
        COUNT(*) as toplam_kayit,
        MIN(ID) as min_id,
        MAX(ID) as max_id
    FROM read_parquet('{parquet_pattern}')
    """
    
    stats = con.execute(query).pl()
    print("📊 İstatistikler:")
    print(stats)
    
    con.close()


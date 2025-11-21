from parquet_sync_manager import ParquetSynchronizer

if __name__ == "__main__":
    
    # 1. Bağlantı Ayarları
    # Lütfen kendi sunucu bilgilerinizi girin:
    # Active Directory ile
    CONN_STR = "mssql://testoltp/Store7?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&trusted_connection=true"

    manager = ParquetSynchronizer(
        db_uri=CONN_STR, 
        out_dir="veri_ambari",  # Parquetlerin duracağı klasör
        chunk=1_000_000               # 1 Milyonluk dilimler
    )

    # --- A. İLK YÜKLEME (Sadece 1 kere çalıştırın) ---
    manager.init(
       table="tb_UrunRecete",
        pk="ID"
    )


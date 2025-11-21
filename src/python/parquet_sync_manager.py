import polars as pl
import os
import math
import json
import time
import logging
from datetime import datetime

class ParquetSynchronizer:
    def __init__(self, db_uri, out_dir, chunk=1_000_000):
        """
        SQL Server ile Parquet dosyaları arasında senkronizasyon sağlar.
        
        Args:
            db_uri (str): 'mssql://user:pass@host/db' formatında bağlantı adresi.
            out_dir (str): Parquet dosyalarının tutulacağı ana dizin.
            chunk (int): ID bazlı dosya bölümleme boyutu (Varsayılan: 1 Milyon).
        """
        self.db_uri = db_uri
        self.out_dir = out_dir
        self.chunk = chunk
        
        # Ana klasör yoksa oluştur
        os.makedirs(self.out_dir, exist_ok=True)
        
        # Logger yapılandırması
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            # Console Handler
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            
            # File Handler
            log_file_path = os.path.join(self.out_dir, "sync_manager.log")
            file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            self.logger.setLevel(logging.INFO)

    def _bucket(self, pk_val):
        """PK değerine göre dosya numarasını (bucket id) hesaplar."""
        if pk_val is None: return 0
        return math.floor(pk_val / self.chunk)

    def _to_hex(self, bytes_val):
        """Binary veriyi SQL sorgusu için Hex stringe (0x...) çevirir."""
        if bytes_val is None:
            return "0x0000000000000000"
        return "0x" + bytes_val.hex()

    def _cols_str(self, cols):
        """Kolon listesini SQL formatına çevirir."""
        if not cols:
            return "*"
        return ", ".join(cols)

    def _paths(self, table):
        """Tabloya özel klasör ve state dosyası yollarını döner."""
        folder_path = os.path.join(self.out_dir, table)
        state_path = os.path.join(self.out_dir, f"{table}_state.json")
        os.makedirs(folder_path, exist_ok=True)
        return folder_path, state_path

    def _read_state(self, state_path, default_val="0x0000000000000000"):
        """En son başarılı işlenen RowVersion'ı JSON dosyasından okur."""
        if os.path.exists(state_path):
            try:
                with open(state_path, "r") as f:
                    data = json.load(f)
                    return data.get("last_rowversion", default_val)
            except Exception:
                return default_val
        return default_val

    def _save_state(self, state_path, last_rv_hex):
        """İşlem hatasız biterse son RowVersion'ı kaydeder."""
        with open(state_path, "w") as f:
            json.dump({
                "last_rowversion": last_rv_hex,
                "last_update": datetime.now().isoformat()
            }, f)

    def init(self, table, pk, cols=None):
        """
        [INITIALIZE] Tabloyu baştan sona okur ve ID aralıklarına göre dosyalar oluşturur.
        """
        folder_path, state_path = self._paths(table)
        select_clause = self._cols_str(cols)
        
        self.logger.info(f"\n🚀 [INIT] '{table}' başlatılıyor...")
        self.logger.info(f"📂 Hedef: {folder_path}")

        # 1. Min/Max ID Bul
        try:
            q_bounds = f"SELECT MIN({pk}) as min_id, MAX({pk}) as max_id FROM {table}"
            bounds_df = pl.read_database_uri(q_bounds, self.db_uri)
            min_id = bounds_df["min_id"][0]
            max_id = bounds_df["max_id"][0]
        except Exception as e:
            self.logger.error(f"❌ Hata: Tablo sınırları okunamadı. {e}")
            return

        if min_id is None:
            self.logger.warning("⚠️ Tablo boş.")
            return

        self.logger.info(f"ℹ️ ID Aralığı: {min_id} - {max_id}")

        # 2. Chunk Döngüsü
        current_start = min_id
        while current_start <= max_id:
            current_end = current_start + self.chunk
            
            query = f"""
            SELECT {select_clause} FROM {table} 
            WHERE {pk} >= {current_start} AND {pk} < {current_end}
            """
            
            try:
                df_chunk = pl.read_database_uri(query, self.db_uri)
                
                if not df_chunk.is_empty():
                    bucket_id = self._bucket(current_start)
                    file_name = f"part_{bucket_id}.parquet"
                    file_path = os.path.join(folder_path, file_name)
                    
                    df_chunk.write_parquet(file_path)
                    self.logger.info(f"  ✅ Yazıldı: {file_name} ({len(df_chunk)} satır)")
            except Exception as e:
                self.logger.error(f"  ❌ Hata ({current_start}-{current_end}): {e}")

            current_start = current_end
            
        self.logger.info(f"🏁 [INIT] '{table}' tamamlandı. Şimdi Upsert çalıştırarak State oluşturabilirsiniz.\n")

    def sync(self, table, pk, ver, cols=None, use_ts=False):
        """
        [INCREMENTAL UPSERT]
        Değişen verileri çeker, ilgili dosyalara dağıtır ve güvenli şekilde günceller.
        Checkpoint dosyası sayesinde işlem yarım kalsa bile veri kaybı olmaz.
        """
        folder_path, state_path = self._paths(table)
        select_clause = self._cols_str(cols)
        
        self.logger.info(f"\n🔄 [UPSERT] '{table}' senkronizasyonu başlıyor...")

        # 1. Checkpoint Oku
        default_rv = "1900-01-01 00:00:00" if use_ts else "0x0000000000000000"
        last_rv_raw = self._read_state(state_path, default_val=default_rv)
        self.logger.info(f"📍 Son Checkpoint: {last_rv_raw}")

        # SQL Sorgusu için değer hazırlama
        if use_ts:
            # Datetime string ise SQL'de tırnak içinde olmalı
            sql_cmp_value = f"'{last_rv_raw}'"
        else:
            # Binary hex string (0x...) tırnaksız kullanılır
            sql_cmp_value = last_rv_raw

        # 2. Delta Veriyi Çek
        # RowVersion binary sıralamasına güveniyoruz.
        sql_query = f"""
        SELECT {select_clause} FROM {table} 
        WHERE {ver} > {sql_cmp_value}
        ORDER BY {ver} ASC
        """
        
        start_time = time.time()
        try:
            df_new = pl.read_database_uri(sql_query, self.db_uri)
        except Exception as e:
            self.logger.error(f"❌ SQL Bağlantı Hatası: {e}")
            return

        if df_new.is_empty():
            self.logger.info("✅ Güncel veri yok. Sistem senkronize.")
            return

        # Bu batch içindeki en büyük RowVersion'ı al (İşlem biterse bunu kaydedeceğiz)
        max_rv_item = df_new.select(pl.col(ver).max()).item()
        
        if use_ts:
            # Datetime objesini stringe çevir
            max_rv_to_save = str(max_rv_item)
        else:
            # Binary objesini hex stringe çevir
            max_rv_to_save = self._to_hex(max_rv_item)
        
        self.logger.info(f"📥 {len(df_new)} kayıt çekildi. ({time.time() - start_time:.2f} sn)")

        # 3. Bucket Dağıtımı ve Upsert
        # Veriyi bucket_id'ye göre sanal olarak böl
        df_new = df_new.with_columns(
            (pl.col(pk) // self.chunk).alias("bucket_id")
        )
        partitions = df_new.partition_by("bucket_id", as_dict=True)

        try:
            for bucket_val, df_updates in partitions.items():
                file_name = f"part_{bucket_val}.parquet"
                file_path = os.path.join(folder_path, file_name)
                
                # Dosyaya yazarken bucket_id gerekmez
                df_updates_clean = df_updates.drop("bucket_id")

                if os.path.exists(file_path):
                    # --- DOSYA GÜNCELLEME (Merge) ---
                    df_current = pl.read_parquet(file_path)
                    
                    # Önce mevcut, sonra yeni veriyi ekle
                    df_combined = pl.concat([df_current, df_updates_clean])
                    
                    # ID'ye göre tekilleştir -> Son geleni (güncel olanı) tut
                    df_final = df_combined.unique(subset=[pk], keep="last", maintain_order=False)
                    
                    df_final.write_parquet(file_path)
                    self.logger.info(f"  ✏️  Güncellendi: {file_name} (Toplam: {len(df_final)})")
                else:
                    # --- YENİ DOSYA ---
                    df_updates_clean.write_parquet(file_path)
                    self.logger.info(f"  ✨ Yeni Dosya: {file_name}")

            # 4. CHECKPOINT KAYDI (Transaction Commit gibi düşünün)
            # Döngü hatasız biterse burası çalışır.
            self._save_state(state_path, max_rv_to_save)
            self.logger.info(f"💾 Checkpoint güncellendi: {max_rv_to_save}")
            
        except Exception as e:
            self.logger.critical(f"❌ KRİTİK HATA: Dosya yazma sırasında sorun oluştu: {e}")
            self.logger.warning("⚠️ Checkpoint GÜNCELLENMEDİ. Bir sonraki çalışmada veriler tekrar çekilip düzeltilecek.")

        self.logger.info(f"🏁 [UPSERT] Bitti. Süre: {time.time() - start_time:.2f} sn\n")


# ==========================================
# ÇALIŞTIRMA BLOĞU (Örnek)
# ==========================================
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

    # --- B. ARTIMLI GÜNCELLEME (Cron/Schedule ile çalıştırın) ---
    #manager.sync(
    #    table="tb_UrunRecete",
    #    pk="SatisId",
    #    ver="RowVersion",
    #    cols=["SatisId", "UrunAdi", "Tutar", "RowVersion"]
    #)

    # --- C. DATETIME ILE ARTIMLI GÜNCELLEME ÖRNEĞİ ---
    # manager.sync(
    #     table="Loglar",
    #     pk="LogId",
    #     ver="CreatedDate",
    #     cols=["LogId", "Message", "CreatedDate"],
    #     use_ts=True
    # )
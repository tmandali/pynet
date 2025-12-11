"""
Örnek çalıştırma dosyası - Bu dosyayı direkt çalıştırabilirsiniz:
    python run_example.py
"""

from db_parquet import (
    S3Config,
    TableConfig,
    DatabaseToParquet,
    get_mssql_connection
)

#pip install s3fs
# s3_config = S3Config(
#     access_key="minioadmin",
#     secret_key="minioadmin",
#     endpoint_url="http://localhost:9000"  # MinIO endpoint
# )

urun_config = TableConfig(
    table_name="tb_Urun",
    columns=[
        'ID', 'UrunID', 'UrunKod', 'Renk', 'Beden', 'Boy', 'Tarih', 'Line',
        'NumuneVaryantSira', 'SergiEkipmanRef', 'SizeRef', 'UrunOptionRef',
        'UrunOptionSizeRef', 'UrunOptionAsortiRef', 'BedenBoyRef', 'Konsinye',
        'AxaAktarilsinmi', 'UrunSKULevelRef', 'LcwArticleCode', 'Agirlik',
        'UrunKoliIcerikTipref'
    ],
    primary_key="ID",
    output_file="urun.parquet"
)

urungrup_config = TableConfig(
    table_name="tb_UrunGrup",
    columns=[
	'ID', 'UrunKod', 'Statu', 'KartTip', 'Ad', 'AmbalajAdet', 'Birim', 
    'Marka', 'YasGrup', 'AnaGrup', 'UstGrup', 'Line', 'Cinsiyet', 'Sezon', 
    'Uretici', 'KdvAlis', 'KdvSatis', 'TeminSekil', 'Malzeme', 'Gram', 'Tarih', 
    'SonDuzenleme', 'ModelNo', 'KritikStok', 'AsgariStok', 'AzamiStok', 'MuhasebeKod', 
    'Mamul', 'Elyaf', 'OzelKod1', 'OzelKod2', 'OzelKod3', 'UrunKlasman', 'Hacim', 'Kampanya', 
    'Alan1', 'Alan2', 'Alan3', 'BedenGrup', 'BoyGrup', 'DonemHaftaKontrol', 'YeniKod', 'YeniKod2K', 
    'MerchGrup', 'VIP', 'Aksesuar', 'HareketTarihi', 'SezonBolum', 'KumasKod', 'AnaKumas', 
    'ReklamUrunu', 'UretilenUlke', 'UrunYonetimGrup', 'ModelOzellik', 'MerchAltGrup', 
    'KumasTip', 'SevkDonemi', 'TahaTeslim', 'YurtIciTeslim', 'UDTeslim', 'NumuneRenk', 
    'DosyaTeslimTR', 'DosyaTeslimEn', 'Tasarimci', 'MerchandiserKod', 'TestOption', 'HayirKurumu', 
    'ModelGrupRef', 'LcwArticleCode'
    ],
    primary_key="ID",
    output_file="urungrup.parquet"
)

sezon_config = TableConfig(
    table_name="tb_Sezon",
    columns=['Kod',	'Yil', 'SezonGrup',	'Tanim', 'Statu', 'Sira', 'KisaKod', 'SezonRef', 'BaslangicTarihi',	'BitisTarihi'],
    primary_key="Kod",
    output_file="sezon.parquet"
)

# Connection string seçin:
# MSSQL_CONNECTION = get_mssql_connection('TEMAOLTP', 'Store7')
# MSSQL_CONNECTION = get_mssql_pyodbc_simple('localhost', 'Store7', 'sa', 'password')
# POSTGRES_CONNECTION = get_postgres_connection('localhost', 'mydb', 'user', 'pass')

MSSQL_CONNECTION = get_mssql_connection('TEMAOLTP', 'Store7')


if __name__ == "__main__":

    for config in [urun_config, urungrup_config, sezon_config]:
        with DatabaseToParquet(
            connection=MSSQL_CONNECTION,
            table_config=config
        ) as converter:
            converter.run()
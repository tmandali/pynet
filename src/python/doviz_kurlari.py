# doviz_kurlari.py (Tarih Desteği Eklenmiş Versiyon)

import requests
import xml.etree.ElementTree as ET
from datetime import date  # Tarih işlemleri için datetime kütüphanesinden date'i import ediyoruz

class TCMBKur:
    """
    TCMB'den belirtilen bir tarihe ait veya günlük döviz kurlarını çeken, 
    saklayan ve sorgulama imkanı sunan bir sınıf.
    """
    def __init__(self, tarih: date = None):
        """
        Nesne oluşturulduğunda otomatik olarak çalışan yapıcı metot (constructor).
        
        Args:
            tarih (datetime.date, optional): Kur verilerinin alınacağı tarih. 
                                            Belirtilmezse, bugünün kurları alınır.
        """
        if tarih is None:
            # Eğer tarih belirtilmemişse, bugünün tarihini kullan
            self.tarih = date.today()
            self.url = "https://www.tcmb.gov.tr/kurlar/today.xml"
        else:
            # Eğer tarih belirtilmişse, o tarihi kullan ve URL'yi formatla
            self.tarih = tarih
            yil_ay = self.tarih.strftime('%Y%m')
            gun_ay_yil = self.tarih.strftime('%d%m%Y')
            self.url = f"https://www.tcmb.gov.tr/kurlar/{yil_ay}/{gun_ay_yil}.xml"

        self.kurlar = {}  # Kur verilerini saklamak için boş bir sözlük
        self._veri_cek_ve_isle() # Veri çekme metodunu çağır

    def _veri_cek_ve_isle(self):
        """
        Veriyi oluşturulan URL'den çeker ve self.kurlar sözlüğünü doldurur.
        """
        try:
            response = requests.get(self.url, timeout=10)
            
            # 404 Not Found hatası, genellikle tatil günlerinde veya veri olmayan günlerde alınır.
            if response.status_code == 404:
                print(f"Uyarı: {self.tarih.strftime('%d.%m.%Y')} tarihi için kur bulunamadı (Hafta sonu veya resmi tatil olabilir).")
                return
            
            # Diğer HTTP hatalarını kontrol et
            response.raise_for_status() # 200 dışındaki durum kodları için hata fırlatır

            root = ET.fromstring(response.content)
            for currency in root.findall('Currency'):
                kod = currency.get('Kod')
                isim = currency.find('Isim').text
                alis = currency.find('ForexBuying').text
                satis = currency.find('ForexSelling').text

                if kod and alis and satis:
                    self.kurlar[kod] = {
                        "isim": isim,
                        "alis": float(alis),
                        "satis": float(satis)
                    }
        except (requests.exceptions.RequestException, ET.ParseError) as e:
            print(f"Veri çekme veya işleme sırasında bir hata oluştu: {e}")
            self.kurlar = {}

    # Diğer metodlar (kurlari_guncelle, tum_kurlari_getir, kur_getir) aynı kalabilir.
    def kurlari_guncelle(self):
        print(f"{self.tarih} tarihi için veriler yeniden güncelleniyor...")
        self._veri_cek_ve_isle()

    def tum_kurlari_getir(self):
        return self.kurlar

    def kur_getir(self, kod):
        kod = kod.upper()
        return self.kurlar.get(kod)


# --- SINIFIN KULLANIM ÖRNEKLERİ ---
if __name__ == "__main__":
    
    # Örnek 1: Bugüne ait kurları al (eski yöntem)
    print("--- 1. Bugünün Kurları ---")
    bugun_kurlari = TCMBKur()
    dolar_bugun = bugun_kurlari.kur_getir("USD")
    if dolar_bugun:
        print(f"Bugün Dolar Alış: {dolar_bugun['alis']} TL")
    
    print("\n" + "="*40 + "\n")

    # Örnek 2: Geçmişteki belirli bir tarihe ait kurları al
    print("--- 2. Geçmiş Tarihli Kurlar (15 Mayıs 2024) ---")
    gecmis_tarih = date(2024, 5, 15) # Yıl, Ay, Gün
    gecmis_kurlar = TCMBKur(tarih=gecmis_tarih)
    dolar_gecmis = gecmis_kurlar.kur_getir("USD")
    
    if dolar_gecmis:
        print(f"{gecmis_tarih} tarihinde Dolar Alış: {dolar_gecmis['alis']} TL")
    
    print("\n" + "="*40 + "\n")

    # Örnek 3: Veri olmayan bir tarih (hafta sonu) için deneme
    print("--- 3. Veri Olmayan Tarih Denemesi (18 Mayıs 2024 - Cumartesi) ---")
    tatil_gunu = date(2024, 5, 18)
    tatil_kurlari = TCMBKur(tarih=tatil_gunu) # Bu işlem sırasında uyarı mesajı basılacak
    
    # Kurlar sözlüğü boş olacağı için sonuç None dönecektir.
    dolar_tatil = tatil_kurlari.kur_getir("USD")
    if not dolar_tatil:
        print("Beklendiği gibi, tatil günü için veri alınamadı.")
import requests
import time
import pandas as pd
from tqdm import tqdm
from login import login_with_sso
import re
import csv

# ------------------------------------------------------
# KONFIGURASI - HARUS DIUPDATE SESUAI STATUS TERBARU
# ------------------------------------------------------
BASE_URL = "https://matchapro.web.bps.go.id/direktori-usaha/data-gc-card"

HEADERS = {
    "host": "matchapro.web.bps.go.id",
    "connection": "keep-alive",
    "sec-ch-ua": "\"Android WebView\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": "\"Android\"",
    "x-requested-with": "com.matchapro.app",
    "user-agent": "Mozilla/5.0 (Linux; Android 12; M2010J19CG Build/SKQ1.211202.001; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/143.0.7499.192 Mobile Safari/537.36",
    "accept": "*/*",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://matchapro.web.bps.go.id",
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    "referer": "https://matchapro.web.bps.go.id/dirgc",
    "accept-language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
    # !!! WAJIB GANTI !!!
    "cookie": "BIGipServeriapps_webhost_mars_443.app~iapps_webhost_mars_443_pool=1418199050.20480.0000; f5avraaaaaaaaaaaaaaaa_session_=ODBGECOKNDHAMKHEONOKBLCFJINBNKOPDOHHPDMAMDLDHOCPPCHDBNDEEDODCNGJBBIDLHHLMONKFPJLCIFAKJKBHHOLEGAPHGDGGEBBPILMPGABINGGGOAAAJHDMMDD; TS0151fc2b=0167a1c8617256103c152b74de844fbe8bb40306e144f77b45b838800bfd5549c6b3891a0ba650753350e75438e59104d825cfd6b1; XSRF-TOKEN=eyJpdiI6InFWaXRuN1IyNE9YSlN3U2dhS2JtZGc9PSIsInZhbHVlIjoiQTNOUVMwOGdPU3g2MThuSVMvYnBQVThicExWbGZ4WmFjRitRQnpDQjVKVlF6d1JVVnJXZkZZcUdBaU8xZExFcmt6SHA0WTVkcFJGVVNyRFB0STJwMlI4MFBYZXRySmVXcnhvc2diV0xUVXF2ZWdHejJ4N29MWW9UZkF4ZXljV0MiLCJtYWMiOiIxYjFjNjEwOTg3YmYyMzIxZWJmZWUyMTYyNDQ4ODkzM2EzNWE4MmU4MGM2YTE5MGU0NzZmMjM5ZDBmM2NlNjY0IiwidGFnIjoiIn0%3D; laravel_session=eyJpdiI6Ilc3SURjbVlBcFZSWkZoZnE0WE1IRmc9PSIsInZhbHVlIjoiaFg3NTRRZWMwa0NCTmEwSGtnZzl2VThMTEQ5Q2tOVmtCRHZSWVJ5bm1LVWNYNTVabHFKR3RhdTlpMG1HOG44bFZEdWZIQ05nR2QzUWs5US9rQXArYTd6ZzNDeEd0ZS9hRWYvTnZNQVFtRzV0VlhjWjFDRnUzcFRYOHU5RThhdkMiLCJtYWMiOiIwODY5NzFhODFjNjM1MjExOTQ0OTExMGMyZDc3ZGY3OTY3NTUzNzBiOGE1MjhlZjMwY2Y4ODRkY2ZjMWY4ODcyIiwidGFnIjoiIn0%3D; TS43cd8bce027=0815dd1fcdab2000ba9e74f32413e1b5a48f183109716470de67437d1b9b44c06139d49ac7102a500866ddda3b11300068f1dc2170d4ad04cdbfbca878a4cedf63d0a8be7544b2549fcb168e2dccf9b1134eeae2987fda42d3d8840e8a6a126a"
}

# Payload dasar
BASE_PAYLOAD = {
    "_token": "",  # Will be set automatically
    "start": 0,
    "length": 1000,           # Kurangi dari 2000 untuk menghindari response terpotong
    "nama_usaha": "",
    "alamat_usaha": "",
    "provinsi": "",        # akan diisi dari parsing cari_kode.htm
    "kabupaten": "",      # akan diisi dari parsing cari_kode.htm
    "kecamatan": "",
    "desa": "",
    "status_filter": "semua",
    "rtotal": "0",
    "sumber_data": "",
    "skala_usaha": "",
    "idsbr": "",
    "history_profiling": ""
}

# Nama file output
OUTPUT_EXCEL = "direktori_usaha_full_all_columns_2026.xlsx"
OUTPUT_CSV_FALLBACK = "direktori_usaha_full_all_columns_2026.csv"

DELAY_BETWEEN_REQUEST = 1.3     # detik, jangan terlalu kecil
# ------------------------------------------------------


def fetch_page(start, length):
    payload = BASE_PAYLOAD.copy()
    payload["start"] = str(start)
    payload["length"] = str(length)

    try:
        r = requests.post(BASE_URL, data=payload, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error saat mengambil data (start={start}): {e}")
        return None


def main():
    print("Melakukan login otomatis...\n")

    # Login credentials - update as needed
    username = input("Masukkan username: ")
    password = input("Masukkan password: ")
    otp_code = input("Masukkan OTP (kosongkan jika tidak ada): ").strip() or None

    # Perform login
    page, browser = login_with_sso(username, password, otp_code)

    if not page:
        print("Login gagal. Tidak dapat melanjutkan scraping.")
        return

    try:
        # Navigate to /dirgc to get _token and parse HTML for provinsi/kabupaten
        url_gc = "https://matchapro.web.bps.go.id/dirgc"
        page.goto(url_gc)
        page.wait_for_load_state('networkidle')

        # Wait for CSRF token meta tag to be attached
        page.wait_for_selector('meta[name="csrf-token"]', state='attached', timeout=10000)

        # Extract _token
        token_element = page.locator('meta[name="csrf-token"]')
        if token_element.count() > 0:
            _token = token_element.get_attribute('content')
            BASE_PAYLOAD["_token"] = _token
            print(f"_token diperoleh: {_token}")
        else:
            print("Gagal mendapatkan _token")
            browser.close()
            return

        # Get cookies
        cookies = page.context.cookies()
        cookie_string = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
        HEADERS["cookie"] = cookie_string
        print("Cookies diperoleh dan diset ke headers")

        # Parse HTML dari request ke direktori-usaha untuk mendapatkan kode provinsi dan kabupaten
        url_direktori = "https://matchapro.web.bps.go.id/direktori-usaha"
        try:
            response = requests.get(url_direktori, headers=HEADERS, timeout=20)
            response.raise_for_status()
            html_content = response.text
            
            # Cari kode provinsi
            prov_match = re.search(r'<select id="f_provinsi".*?<option value="(\d+)" selected>', html_content, re.DOTALL)
            if prov_match:
                kode_provinsi_ver_matchapro = prov_match.group(1)
            else:
                kode_provinsi_ver_matchapro = ""  # default BALI
            
            # Cari kode kabupaten
            kab_match = re.search(r'<select id="f_kabupaten".*?<option value="(\d+)" selected>', html_content, re.DOTALL)
            if kab_match:
                kode_kabupaten_ver_matchapro = kab_match.group(1)
            else:
                kode_kabupaten_ver_matchapro = ""  # default BADUNG
            
            # Update BASE_PAYLOAD
            BASE_PAYLOAD["provinsi"] = kode_provinsi_ver_matchapro
            BASE_PAYLOAD["kabupaten"] = kode_kabupaten_ver_matchapro
            
            print(f"Kode provinsi: {kode_provinsi_ver_matchapro}")
            print(f"Kode kabupaten: {kode_kabupaten_ver_matchapro}")
        
        except Exception as e:
            print(f"Error saat parsing HTML dari direktori-usaha: {e}")
            # Gunakan default
            BASE_PAYLOAD["provinsi"] = ""
            BASE_PAYLOAD["kabupaten"] = ""

    except Exception as e:
        print(f"Error saat login atau ekstraksi: {e}")
        browser.close()
        return

    # Close browser after getting credentials
    browser.close()

    print("Login berhasil. Memulai pengambilan data...\n")

    # Cek total data & ambil response pertama
    first_response = fetch_page(0, 100)  # Gunakan 100 untuk cek total
    if not first_response or "recordsTotal" not in first_response:
        print("Gagal mendapatkan informasi awal.")
        print("Periksa kembali autentikasi dan koneksi internet")
        return

    total_records = first_response["recordsTotal"]
    print(f"Total data yang tersedia : {total_records:,} record")
    print(f"Output akan disimpan ke : {OUTPUT_CSV_FALLBACK}\n")

    all_records = []
    length_per_request = 1000  # Kurangi dari 2000 untuk menghindari response terpotong

    with tqdm(total=total_records, desc="Progress", unit="record") as pbar:
        start = 0
        while start < total_records:
            data = fetch_page(start, length_per_request)

            if not data or "data" not in data or not isinstance(data["data"], list):
                print(f"\nGagal di posisi start={start}. Mencoba lagi setelah jeda...")
                time.sleep(6)
                continue

            page_data = data["data"]
            all_records.extend(page_data)

            fetched_this_time = len(page_data)
            pbar.update(fetched_this_time)

            start += fetched_this_time

            time.sleep(DELAY_BETWEEN_REQUEST)

    if not all_records:
        print("\nTidak ada data yang berhasil dikumpulkan.")
        return

    print(f"\nSelesai mengumpulkan {len(all_records):,} record")

    # Bersihkan data dari karakter newline dan tab yang bisa merusak CSV (khusus alamat_usaha, kegiatan_usaha, dan nama_usaha)
    for record in all_records:
        if 'alamat_usaha' in record and isinstance(record['alamat_usaha'], str):
            record['alamat_usaha'] = record['alamat_usaha'].replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
        if 'kegiatan_usaha' in record and isinstance(record['kegiatan_usaha'], str):
            record['kegiatan_usaha'] = record['kegiatan_usaha'].replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
        if 'nama_usaha' in record and isinstance(record['nama_usaha'], str):
            record['nama_usaha'] = record['nama_usaha'].replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')

    # Jadikan DataFrame (semua kolom otomatis ikut)
    df = pd.DataFrame(all_records)

    print(f"Jumlah kolom yang didapat: {len(df.columns)}")
    print("Nama kolom:", ", ".join(df.columns.tolist()))

    # Simpan ke CSV
    try:
        df.to_csv(OUTPUT_CSV_FALLBACK, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        print(f"\nBerhasil disimpan ke: {OUTPUT_CSV_FALLBACK}")
        print(f"\nTips: Jika membuka csv di excel pilih dont convert")
        print(f"\nData hasil download dari matchapro ini merupakan data sesudah dan sebelum profiling, wajin diolah terlebih dahulu sebelum dikirim")
        print(f"\nPENTING: Sebelum melakukan pengiriman GC, dipastikan data sudah valid, pastikan format koordinat dan kode hasilgc sudah sesuai")
    except Exception as e:
        print(f"Gagal menyimpan CSV: {e}")


if __name__ == "__main__":
    main()
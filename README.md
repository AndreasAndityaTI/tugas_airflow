# ETL Pipeline Sederhana — Sample Superstore (Apache Airflow)

**Mahasiswa:** Andreas Anditya Purnama  
**NIM:** 572392  
**Mata Kuliah:** Data Warehouse dan Business Intelligence  

---

## Deskripsi Proyek

Pipeline ETL sederhana menggunakan Apache Airflow untuk memproses dataset **Sample Superstore** (9.994 baris penjualan ritel AS dari Kaggle). Pipeline terdiri dari 4 task berurutan:

```
extract_data → validate_data → transform_data → load_data
```

| Task | Fungsi |
|------|--------|
| `extract_data` | Baca `data/raw/superstore.csv`, tulis ke staging (`data/staging/superstore_staging.csv`) |
| `validate_data` | Null check kolom kritis + duplicate check (hanya logging, tidak menghentikan pipeline) |
| `transform_data` | Agregasi wajib: daily sales + category summary. Bonus: region summary, top 10 products, monthly trend |
| `load_data` | Simpan ke SQLite (`superstore_dw.sqlite`), buat visualisasi matplotlib (`sales_overview.png`) |

---

## Struktur Direktori

```
572392_Andreas_AirflowETL/
├── code/                          # Source code utama
│   ├── dags/
│   │   └── etl_sederhana_572392.py   # DAG Airflow
│   ├── data/
│   │   ├── raw/superstore.csv         # Dataset sumber (harus disediakan)
│   │   ├── staging/                   # Output extract (auto-generated)
│   │   └── output/                    # Output transform+load (auto-generated)
│   ├── airflow_home/                  # Konfigurasi Airflow
│   ├── requirements.txt               # Dependencies Python
│   ├── visualize_dag.py               # Script visualisasi DAG (matplotlib)
│   └── dag_visualization.png          # Hasil visualisasi DAG
├── docs/
│   └── Dokumentasi_ETL_Airflow_572392.pdf
└── README.md                        # File ini
```

---

## Persiapan Sebelum Menjalankan

### 1. Dataset
Download dataset **Sample Superstore** dari Kaggle:
- URL: https://www.kaggle.com/datasets/vivek468/superstore-dataset-final
- Simpan file `superstore.csv` ke: `code/data/raw/superstore.csv`

### 2. Requirements
File `code/requirements.txt`:
```
apache-airflow==2.9.2
pandas==2.2.2
matplotlib==3.8.4
```

---

## Langkah-Langkah Menjalankan (Step-by-Step)

### OPSI A: Menjalankan Tanpa Airflow (Cepat, untuk Testing)

```bash
# 1. Masuk ke folder code
cd /Users/andreasanditya/Downloads/572392_Andreas_AirflowETL/code

# 2. Buat virtual environment
python3 -m venv .venv

# 3. Aktifkan virtual environment
source .venv/bin/activate
# Windows: .venv\Scripts\activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Set environment variable project root
export SUPERSTORE_PROJECT_ROOT="$(pwd)"

# 6. Jalankan seluruh pipeline ETL
python3 -c "
from dags.etl_sederhana_572392 import extract_data, validate_data, transform_data, load_data
extract_data()
validate_data()
transform_data()
load_data()
"

# 7. Verifikasi output
ls -la data/staging/
ls -la data/output/

# 8. (Opsional) Jalankan ulang untuk membuktikan idempotency (re-run aman)
python3 -c "
from dags.etl_sederhana_572392 import extract_data, validate_data, transform_data, load_data
extract_data()
validate_data()
transform_data()
load_data()
"
```

**Output yang diharapkan di `data/output/`:**
- `daily_sales.csv` — Agregasi harian (1.236 hari)
- `category_summary.csv` — Ringkasan per kategori (3 kategori)
- `region_summary.csv` — Performa per region (4 region)
- `top_products.csv` — 10 produk terlaris
- `monthly_trend.csv` — Tren bulanan (48 bulan)
- `superstore_dw.sqlite` — Database SQLite (3 tabel + index)
- `sales_overview.png` — Visualisasi matplotlib

---

### OPSI B: Menjalankan di Apache Airflow (Produksi)

```bash
# 1. Masuk ke folder code
cd /Users/andreasanditya/Downloads/572392_Andreas_AirflowETL/code

# 2. Aktifkan virtual environment (jika belum)
source .venv/bin/activate

# 3. Set environment variables
export AIRFLOW_HOME="$(pwd)/airflow_home"
export SUPERSTORE_PROJECT_ROOT="$(pwd)"

# 4. Inisialisasi database Airflow
airflow db migrate

# 5. Buat user admin Airflow
airflow users create \
  --username admin \
  --password admin \
  --firstname A \
  --lastname B \
  --role Admin \
  --email admin@example.com

# 6. Pastikan DAG sudah di folder dags Airflow
mkdir -p "$AIRFLOW_HOME/dags"
cp dags/etl_sederhana_572392.py "$AIRFLOW_HOME/dags/"

# 7. Jalankan Airflow standalone (webserver + scheduler)
airflow standalone
```

**Akses Airflow UI:**
- Buka browser: http://localhost:8080
- Login: `admin` / `admin`
- Cari DAG: `etl_sederhana_572392`
- Klik **Unpause** (toggle ON)
- Klik **Trigger DAG** ▶️
- Lihat **Graph View** untuk visualisasi dependency
- Klik task → **Log** untuk melihat output per task

---

### OPSI C: Visualisasi DAG (Matplotlib)

```bash
# 1. Masuk ke folder code
cd /Users/andreasanditya/Downloads/572392_Andreas_AirflowETL/code

# 2. Aktifkan virtual environment
source .venv/bin/activate

# 3. Jalankan script visualisasi
python3 visualize_dag.py

# 4. Hasil: dag_visualization.png
ls -la dag_visualization.png
```

---

## Output Lengkap Pipeline

| File | Lokasi | Deskripsi |
|------|--------|-----------|
| `superstore_staging.csv` | `data/staging/` | Hasil extract (9.994 baris, 21 kolom) |
| `daily_sales.csv` | `data/output/` | Agregasi harian: order_count, line_count, total_sales, total_profit, total_qty, avg_discount |
| `category_summary.csv` | `data/output/` | Rata-rata sales/profit per kategori (Furniture, Office Supplies, Technology) |
| `region_summary.csv` | `data/output/` | **Bonus** — Performa per region (Central, East, South, West) |
| `top_products.csv` | `data/output/` | **Bonus** — 10 produk terlaris by total_sales |
| `monthly_trend.csv` | `data/output/` | **Bonus** — Tren bulanan sales, profit, orders |
| `superstore_dw.sqlite` | `data/output/` | Data warehouse SQLite: 3 tabel (`superstore_daily`, `superstore_category`, `superstore_monthly`) + index |
| `sales_overview.png` | `data/output/` | Visualisasi: (kiri) tren bulanan, (kanan) bar chart kategori |

---

## Detail Teknis

### Kolom Kritis yang Divisualisasikan (validate_data)
- Order ID, Order Date, Customer ID, Sales, Quantity, Profit, Postal Code

### Konfigurasi DAG
```python
dag_id = "etl_sederhana_572392"
schedule_interval = "@daily"
start_date = datetime(2025, 1, 1)
catchup = False
tags = ["ugm", "dwh", "superstore", "etl"]
```

### Default Args
- `retries`: 1
- `retry_delay`: 2 menit
- `owner`: "Andreas Anditya Purnama"

---

## Troubleshooting

| Masalah | Solusi |
|---------|--------|
| `FileNotFoundError: superstore.csv` | Pastikan file ada di `code/data/raw/superstore.csv` |
| `ModuleNotFoundError` | Jalankan `pip install -r requirements.txt` di dalam `.venv` |
| Airflow UI tidak bisa dibuka | Pastikan port 8080 tidak digunakan, cek `airflow standalone` logs |
| DAG tidak muncul | Pastikan file `.py` di `$AIRFLOW_HOME/dags/`, restart webserver |
| Permission denied SQLite | Pastikan folder `data/output/` writable |

---

## Bonus yang Diimplementasikan

- **Transformasi tambahan:** `region_summary`, `top_products` (10), `monthly_trend`
- **Logging terstruktur:** Format `[task_id] pesan` konsisten di semua task
- **Visualisasi matplotlib:** `sales_overview.png` (dark theme, tren bulanan + bar kategori)
- **Idempotent pipeline:** Bisa di-re-run berkali-kali tanpa duplikasi data
- **SQLite warehouse:** 3 tabel terindeks siap query analitik
- **DAG visualization:** Script `visualize_dag.py` generate graf dependency

---

**Total baris kode DAG:** 365 baris  
**Dataset:** 9.994 records (melebihi minimum 200 records)# tugas_airflow

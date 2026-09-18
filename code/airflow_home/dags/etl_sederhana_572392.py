"""

Dataset     : Sample Superstore
              https://www.kaggle.com/datasets/vivek468/superstore-dataset-final


"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

LOGGER = logging.getLogger("etl_sederhana")
LOGGER.setLevel(logging.INFO)

PROJECT_ROOT = Path(
    os.environ.get("SUPERSTORE_PROJECT_ROOT", Path(__file__).resolve().parents[1])
)
RAW_PATH = PROJECT_ROOT / "data" / "raw" / "superstore.csv"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"
STAGING_PATH = STAGING_DIR / "superstore_staging.csv"
SQLITE_PATH = OUTPUT_DIR / "superstore_dw.sqlite"

CRITICAL_COLUMNS = [
    "Order ID",
    "Order Date",
    "Customer ID",
    "Sales",
    "Quantity",
    "Profit",
    "Postal Code",
]


def _ensure_dirs() -> None:
    """Buat folder staging dan output jika belum ada."""
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _log_banner(task_id: str, message: str) -> None:
    """Format log terstruktur yang konsisten di semua task."""
    LOGGER.info("[%s] %s", task_id, message)


def extract_data(**context) -> str:
    """Task 1 — Extract.

    Membaca CSV sumber Sample Superstore lalu menyalinnya ke folder
    staging. File staging menjadi kontrak input bagi task berikutnya
    sehingga extract bisa di-rerun tanpa menyentuh file raw.
    """
    _ensure_dirs()
    _log_banner("extract_data", f"Membaca sumber: {RAW_PATH}")

    if not RAW_PATH.exists():
        raise FileNotFoundError(
            f"File sumber tidak ditemukan: {RAW_PATH}. "
            "Pastikan superstore.csv ada di data/raw/."
        )

    df = pd.read_csv(RAW_PATH)
    _log_banner(
        "extract_data",
        f"Berhasil dibaca: {len(df):,} baris, {df.shape[1]} kolom",
    )
    _log_banner("extract_data", f"Kolom: {list(df.columns)}")

    df.to_csv(STAGING_PATH, index=False)
    _log_banner("extract_data", f"Staging ditulis: {STAGING_PATH}")
    return str(STAGING_PATH)


def validate_data(**context) -> None:
    """Task 2 — Validate.

    Dua pengecekan kualitas wajib:
      1. Null value check pada kolom penting
      2. Duplicate records check (baris penuh + kombinasi Order ID + Product ID)

    Temuan hanya dicatat lewat logging. Pipeline tidak dihentikan
    agar proses transformasi tetap menghasilkan output analitik.
    """
    _log_banner("validate_data", f"Membaca staging: {STAGING_PATH}")
    df = pd.read_csv(STAGING_PATH)
    n_rows = len(df)
    _log_banner("validate_data", f"Dataset staging: {n_rows:,} baris")

    # --- 1. Null value check ---
    _log_banner("validate_data", "=== NULL VALUE CHECK ===")
    total_nulls = 0
    for col in CRITICAL_COLUMNS:
        if col not in df.columns:
            LOGGER.warning("[validate_data] Kolom '%s' tidak ada di dataset", col)
            continue
        null_count = int(df[col].isna().sum()) + int(
            (df[col].astype(str).str.strip() == "").sum()
        )
        pct = (null_count / n_rows * 100) if n_rows else 0
        total_nulls += null_count
        _log_banner(
            "validate_data",
            f"Null '{col}': {null_count} baris ({pct:.2f}%)",
        )
        if null_count > 0:
            LOGGER.warning(
                "[validate_data] Ditemukan nilai kosong pada '%s' — dicatat, pipeline dilanjutkan",
                col,
            )

    # --- 2. Duplicate records check ---
    _log_banner("validate_data", "=== DUPLICATE RECORDS CHECK ===")
    full_dups = int(df.duplicated().sum())
    subset_cols = [c for c in ["Order ID", "Product ID"] if c in df.columns]
    subset_dups = (
        int(df.duplicated(subset=subset_cols).sum()) if subset_cols else 0
    )

    _log_banner("validate_data", f"Duplikat baris penuh: {full_dups}")
    _log_banner(
        "validate_data",
        f"Duplikat (Order ID + Product ID): {subset_dups}",
    )
    if full_dups or subset_dups:
        LOGGER.warning(
            "[validate_data] Duplikat terdeteksi — tidak menghapus baris, hanya dicatat",
        )

    _log_banner(
        "validate_data",
        f"Ringkasan kualitas: null_issues={total_nulls}, "
        f"full_dups={full_dups}, order_product_dups={subset_dups}",
    )
    _log_banner("validate_data", "Validasi selesai. Pipeline dilanjutkan ke transform.")


def transform_data(**context) -> None:
    """Task 3 — Transform.

    Agregasi wajib:
      - total penjualan harian
      - rata-rata sales/profit per kategori

    Agregasi tambahan (bonus):
      - performa per region
      - 10 produk terlaris
      - tren bulanan
    """
    _ensure_dirs()
    _log_banner("transform_data", f"Membaca staging: {STAGING_PATH}")
    df = pd.read_csv(STAGING_PATH)
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
    df = df.dropna(subset=["Order Date"])

    # 1. Total per hari
    daily = (
        df.groupby(df["Order Date"].dt.date)
        .agg(
            order_count=("Order ID", "nunique"),
            line_count=("Row ID", "count"),
            total_sales=("Sales", "sum"),
            total_profit=("Profit", "sum"),
            total_qty=("Quantity", "sum"),
            avg_discount=("Discount", "mean"),
        )
        .reset_index()
        .rename(columns={"Order Date": "order_date"})
        .sort_values("order_date")
    )
    daily_path = OUTPUT_DIR / "daily_sales.csv"
    daily.to_csv(daily_path, index=False)
    _log_banner(
        "transform_data",
        f"Agregasi harian: {len(daily):,} hari -> {daily_path}",
    )

    # 2. Rata-rata per kategori
    category = (
        df.groupby("Category")
        .agg(
            items=("Row ID", "count"),
            avg_sales=("Sales", "mean"),
            avg_profit=("Profit", "mean"),
            total_sales=("Sales", "sum"),
            total_profit=("Profit", "sum"),
        )
        .reset_index()
        .sort_values("total_sales", ascending=False)
    )
    category_path = OUTPUT_DIR / "category_summary.csv"
    category.to_csv(category_path, index=False)
    _log_banner(
        "transform_data",
        f"Agregasi kategori: {len(category)} kategori -> {category_path}",
    )

    # 3. Bonus — performa region
    region = (
        df.groupby("Region")
        .agg(
            items=("Row ID", "count"),
            total_sales=("Sales", "sum"),
            total_profit=("Profit", "sum"),
            avg_discount=("Discount", "mean"),
        )
        .reset_index()
        .sort_values("total_sales", ascending=False)
    )
    region.to_csv(OUTPUT_DIR / "region_summary.csv", index=False)
    _log_banner("transform_data", "Bonus: region_summary.csv ditulis")

    # 4. Bonus — 10 produk terlaris
    top_products = (
        df.groupby(["Product Name", "Category"])
        .agg(
            total_sales=("Sales", "sum"),
            total_profit=("Profit", "sum"),
            quantity=("Quantity", "sum"),
        )
        .reset_index()
        .sort_values("total_sales", ascending=False)
        .head(10)
    )
    top_products.to_csv(OUTPUT_DIR / "top_products.csv", index=False)
    _log_banner("transform_data", "Bonus: top_products.csv (10 baris) ditulis")

    # 5. Bonus — tren bulanan
    monthly = (
        df.groupby(df["Order Date"].dt.to_period("M").astype(str))
        .agg(
            total_sales=("Sales", "sum"),
            total_profit=("Profit", "sum"),
            orders=("Order ID", "nunique"),
        )
        .reset_index()
        .rename(columns={"Order Date": "month"})
        .sort_values("month")
    )
    monthly.to_csv(OUTPUT_DIR / "monthly_trend.csv", index=False)
    _log_banner(
        "transform_data", f"Bonus: monthly_trend.csv ({len(monthly)} bulan)"
    )


def load_data(**context) -> None:
    """Task 4 — Load.

    Menyimpan hasil transformasi ke CSV (sudah ditulis di transform)
    dan ke tabel SQLite. Menambah visualisasi matplotlib sebagai
    artefak analitik siap pakai.
    """
    _ensure_dirs()
    daily_path = OUTPUT_DIR / "daily_sales.csv"
    category_path = OUTPUT_DIR / "category_summary.csv"
    monthly_path = OUTPUT_DIR / "monthly_trend.csv"

    daily = pd.read_csv(daily_path)
    category = pd.read_csv(category_path)
    monthly = pd.read_csv(monthly_path)

    _log_banner("load_data", f"Memuat {len(daily):,} baris agregasi harian")
    _log_banner("load_data", f"Memuat {len(category)} ringkasan kategori")

    if SQLITE_PATH.exists():
        SQLITE_PATH.unlink()

    with sqlite3.connect(SQLITE_PATH) as conn:
        daily.to_sql("superstore_daily", conn, index=False, if_exists="replace")
        category.to_sql(
            "superstore_category", conn, index=False, if_exists="replace"
        )
        monthly.to_sql(
            "superstore_monthly", conn, index=False, if_exists="replace"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_daily_date ON superstore_daily(order_date)"
        )
    _log_banner("load_data", f"SQLite warehouse: {SQLITE_PATH}")

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), dpi=140)
    fig.patch.set_facecolor("#0b0d10")
    for ax in axes:
        ax.set_facecolor("#14181d")
        ax.tick_params(colors="#8b9298")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#2a323a")
        ax.spines["bottom"].set_color("#2a323a")
        ax.yaxis.label.set_color("#ececea")
        ax.xaxis.label.set_color("#ececea")
        ax.title.set_color("#ececea")

    axes[0].plot(
        pd.to_datetime(monthly["month"]),
        monthly["total_sales"],
        color="#c5cdc8",
        linewidth=2,
    )
    axes[0].set_title("Tren penjualan bulanan")
    axes[0].set_ylabel("Sales (USD)")

    axes[1].bar(
        category["Category"],
        category["total_sales"],
        color=["#c5cdc8", "#7d9b86", "#8fa3b5"],
    )
    axes[1].set_title("Total sales per kategori")
    axes[1].set_ylabel("Sales (USD)")

    fig.tight_layout()
    chart_path = OUTPUT_DIR / "sales_overview.png"
    fig.savefig(chart_path, facecolor=fig.get_facecolor())
    plt.close(fig)
    _log_banner("load_data", f"Grafik disimpan: {chart_path}")
    _log_banner("load_data", "Load selesai. Output siap dianalisis.")


default_args = {
    "owner": "Andreas Anditya Purnama",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="etl_sederhana_572392",
    description="ETL sederhana Sample Superstore — extract, validate, transform, load",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ugm", "dwh", "superstore", "etl"],
) as dag:
    t_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )
    t_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )
    t_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )
    t_load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    t_extract >> t_validate >> t_transform >> t_load

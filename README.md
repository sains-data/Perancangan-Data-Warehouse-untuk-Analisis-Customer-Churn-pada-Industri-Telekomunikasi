# 📊 Data Warehouse: Analisis Customer Churn Telekomunikasi
**Kelompok 8 – DW RA**

---

## 🧾 Ringkasan Proyek
Proyek ini membangun sistem **Data Warehouse berbasis SQL Server** untuk menganalisis perilaku *customer churn* di industri telekomunikasi. Sistem ini mendukung proses **ETL**, penerapan **skema bintang**, dan **query OLAP** untuk pengambilan keputusan strategis.

---

## 🎯 Tujuan dan Ruang Lingkup

- Menganalisis churn pelanggan untuk mendukung strategi retensi
- Menghitung metrik seperti **ARPU**, **tren churn**, dan identifikasi layanan berisiko
- Menggunakan skema bintang (1 tabel fakta & 6 dimensi)
- ETL dilakukan dengan **Python** dan **SQL Server**

---

## 🔧 Metodologi

Menggunakan pendekatan **Waterfall** yang mencakup:
1. Identifikasi kebutuhan bisnis dan stakeholder
2. Desain konseptual dan logikal Data Warehouse
3. Implementasi fisik dan optimasi indexing/partisi
4. Proses ETL, query OLAP, dan dokumentasi akhir

---

## 📦 Dataset

Dataset yang digunakan adalah `telecom_customer_churn.csv` dari Kaggle, memuat **7.043 baris data pelanggan** dari kuartal II tahun 2022. Dataset ini mencakup informasi demografis, jenis layanan, kontrak, pembayaran, dan status churn pelanggan.

---

## 🗂️ Struktur Skema

- **Tabel Fakta**:
  - `fact_customer_churn`

- **Tabel Dimensi**:
  - `dim_customer`
  - `dim_service`
  - `dim_payment`
  - `dim_location`
  - `dim_date`
  - `dim_churn`

---

## 🔁 Proses ETL

- Pembersihan data: mengatasi nilai kosong dan tidak valid
- Transformasi kolom:
  - `ARPU` = Total Revenue / Tenure in Months
  - `churn_flag`: 1 jika churn, 0 jika aktif
  - Kolom waktu: `churn_year`, `churn_month`, `churn_quarter`
- Pemuatan data ke SQL Server menggunakan `insert_data.sql`

---

## 🔍 OLAP Queries

import pandas as pd
import mysql.connector

# 1. KONEKSI DATABASE
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='dw_telkom'
)
cursor = conn.cursor()

# 2. MEMBUAT TABEL-TABEL
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    gender VARCHAR(10),
    senior_citizen TINYINT,
    partner VARCHAR(10),
    dependents VARCHAR(10)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_service (
    service_id INT AUTO_INCREMENT PRIMARY KEY,
    phone_service TINYINT,
    multiple_lines VARCHAR(20),
    internet_service TINYINT,
    online_security VARCHAR(20),
    online_backup VARCHAR(20),
    device_protection VARCHAR(20),
    tech_support VARCHAR(20),
    streaming_tv VARCHAR(20),
    streaming_movies VARCHAR(20),
    contract VARCHAR(20),
    paperless_billing VARCHAR(10)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_payment (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,
    payment_method VARCHAR(100)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_location (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    churn_year INT,
    churn_month VARCHAR(20),
    churn_quarter VARCHAR(10)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_churn (
    churn_flag_id INT AUTO_INCREMENT PRIMARY KEY,
    churn_flag TINYINT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS fact_customer_churn (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50),
    service_id INT,
    payment_id INT,
    location_id INT,
    date_id INT,
    churn_flag_id INT,
    tenure INT,
    monthly_charges FLOAT,
    total_charges FLOAT,
    arpu FLOAT,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (service_id) REFERENCES dim_service(service_id),
    FOREIGN KEY (payment_id) REFERENCES dim_payment(payment_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (churn_flag_id) REFERENCES dim_churn(churn_flag_id)
)
""")

# 3. TRANSFORMASI DATA
df = pd.read_csv(r"C:\\dw\\telecom_customer_churn.csv")
df.dropna(inplace=True)

# Konversi dan perhitungan kolom tambahan
df["Churn_Flag"] = df["Customer Status"].apply(lambda x: 1 if x == "Churned" else 0)
df['Senior Citizen'] = df['Age'].apply(lambda x: 1 if x >= 65 else 0)
df["Tenure in Months"] = df["Tenure in Months"].replace(0, 1)
df["Total Charges"] = pd.to_numeric(df["Total Charges"], errors='coerce')
df["ARPU"] = df["Total Revenue"] / df["Tenure in Months"]
df = df[df['Total Charges'] >= 0]
df['Phone Service'] = df['Phone Service'].apply(lambda x: 1 if x == 'Yes' else 0)
df['Internet Service'] = df['Internet Service'].apply(lambda x: 1 if x == 'Yes' else 0)

df['churn_year'] = 2022
df['churn_month'] = 'June'
df['churn_quarter'] = 'Q2'

# 4. KOSONGKAN TABEL SEBELUM INSERT
cursor.execute("DELETE FROM fact_customer_churn")
cursor.execute("DELETE FROM dim_churn")
cursor.execute("DELETE FROM dim_date")
cursor.execute("DELETE FROM dim_payment")
cursor.execute("DELETE FROM dim_service")
cursor.execute("DELETE FROM dim_customer")
cursor.execute("DELETE FROM dim_location")

# 5. ISI TABEL DIMENSI
# dim_churn
for flag in [0, 1]:
    cursor.execute("INSERT INTO dim_churn (churn_flag) VALUES (%s)", (flag,))

# dim_date
cursor.execute("INSERT INTO dim_date (churn_year, churn_month, churn_quarter) VALUES (%s, %s, %s)", (2022, 'June', 'Q2'))

# dim_payment
payment_methods = df['Payment Method'].unique()
for method in payment_methods:
    cursor.execute("INSERT INTO dim_payment (payment_method) VALUES (%s)", (method,))

# dim_customer
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO dim_customer (customer_id, gender, senior_citizen, partner, dependents)
        VALUES (%s, %s, %s, %s, %s)
    """, (row['Customer ID'], row['Gender'], row['Senior Citizen'], row['Married'], row['Number of Dependents']))

# dim_service
# Normalisasi nilai string (strip spasi dan lower untuk kolom kategori)
def normalize_str(val):
    if isinstance(val, str):
        return val.strip().lower()
    return val

service_df = df[[
    'Phone Service', 'Multiple Lines', 'Internet Service', 'Online Security',
    'Online Backup', 'Device Protection Plan', 'Premium Tech Support',
    'Streaming TV', 'Streaming Movies', 'Contract', 'Paperless Billing'
]].drop_duplicates()

service_df['Multiple Lines'] = service_df['Multiple Lines'].apply(normalize_str)
service_df['Online Security'] = service_df['Online Security'].apply(normalize_str)
service_df['Online Backup'] = service_df['Online Backup'].apply(normalize_str)
service_df['Device Protection Plan'] = service_df['Device Protection Plan'].apply(normalize_str)
service_df['Premium Tech Support'] = service_df['Premium Tech Support'].apply(normalize_str)
service_df['Streaming TV'] = service_df['Streaming TV'].apply(normalize_str)
service_df['Streaming Movies'] = service_df['Streaming Movies'].apply(normalize_str)
service_df['Contract'] = service_df['Contract'].apply(normalize_str)
service_df['Paperless Billing'] = service_df['Paperless Billing'].apply(normalize_str)

for _, row in service_df.iterrows():
    cursor.execute("""
        INSERT INTO dim_service (
            phone_service, multiple_lines, internet_service, online_security,
            online_backup, device_protection, tech_support, streaming_tv,
            streaming_movies, contract, paperless_billing
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['Phone Service'],
        row['Multiple Lines'],
        row['Internet Service'],
        row['Online Security'],
        row['Online Backup'],
        row['Device Protection Plan'],
        row['Premium Tech Support'],
        row['Streaming TV'],
        row['Streaming Movies'],
        row['Contract'],
        row['Paperless Billing']
    ))

# dim_location
cities = df['City'].unique()
for city in cities:
    cursor.execute("INSERT INTO dim_location (city) VALUES (%s)", (city,))

# Ambil ID referensi
cursor.execute("SELECT date_id FROM dim_date LIMIT 1")
date_id = cursor.fetchone()[0]

# Buat cache ID untuk mapping dimensi
cursor.execute("SELECT churn_flag_id, churn_flag FROM dim_churn")
rows = cursor.fetchall()
churn_map = {churn_flag: churn_flag_id for churn_flag_id, churn_flag in rows}

cursor.execute("SELECT payment_id, payment_method FROM dim_payment")
payment_map = {method: pid for pid, method in cursor.fetchall()}

cursor.execute("SELECT location_id, city FROM dim_location")
location_map = {city: lid for lid, city in cursor.fetchall()}

cursor.execute("SELECT * FROM dim_service")
services = cursor.fetchall()

# Buat mapping service_id dari row dim_service
service_map = {}
for s in services:
    sid, *fields = s
    # Normalisasi juga saat matching
    normalized_fields = []
    for f in fields:
        if isinstance(f, str):
            normalized_fields.append(f.strip().lower())
        else:
            normalized_fields.append(f)
    service_map[tuple(normalized_fields)] = sid

# 6. INSERT KE fact_customer_churn DENGAN VALIDASI
for _, row in df.iterrows():
    service_key = (
        row['Phone Service'],
        normalize_str(row['Multiple Lines']),
        row['Internet Service'],
        normalize_str(row['Online Security']),
        normalize_str(row['Online Backup']),
        normalize_str(row['Device Protection Plan']),
        normalize_str(row['Premium Tech Support']),
        normalize_str(row['Streaming TV']),
        normalize_str(row['Streaming Movies']),
        normalize_str(row['Contract']),
        normalize_str(row['Paperless Billing'])
    )
    service_id = service_map.get(service_key)
    payment_id = payment_map.get(row['Payment Method'])
    churn_flag_id = churn_map.get(row['Churn_Flag'])
    location_id = location_map.get(row['City'])

    print(f"Customer ID: {row['Customer ID']}, Churn_Flag: {row['Churn_Flag']}, Mapped churn_flag_id: {churn_flag_id}")

    cursor.execute("""
        INSERT INTO fact_customer_churn (
            customer_id, service_id, payment_id, location_id,
            date_id, churn_flag_id, tenure, monthly_charges, total_charges, arpu
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['Customer ID'], service_id, payment_id, location_id,
        date_id, churn_flag_id, row['Tenure in Months'], row['Monthly Charge'],
        row['Total Charges'], row['ARPU']
    ))

# Commit dan tutup koneksi
conn.commit()
conn.close()
print("ETL selesai. Semua tabel berhasil dibuat dan data dimuat ke MySQL.")

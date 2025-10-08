from __future__ import annotations
import pandas as pd
import requests
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os

# --- 1. แก้ไขฟังก์ชัน Extract ใหม่ทั้งหมด ---
def extract_csv_data(**kwargs):
    """ดึงข้อมูลโดยการอ่านไฟล์ CSV จาก URL โดยตรง"""
    print("Starting data extraction from CSV URL...")
    
    # FIX: URL นี้ชี้ไปยังไฟล์ CSV โดยตรง
    csv_url = "https://nakhonpathom.gdcatalog.go.th/dataset/94c220e2-c38b-494c-8269-a0748a063a46/resource/139dde01-82ab-423a-a045-54da9bf3370e/download/untitled.csv"
    
    try:
        # FIX: ใช้วิธีที่ถูกต้องคือใช้ Pandas อ่าน CSV จาก URL
        df = pd.read_csv(csv_url)
        print(f"Successfully read {len(df)} rows from CSV.")
        
        # FIX: แปลง DataFrame เป็น list of dictionaries เพื่อส่งต่อให้ Task Transform
        # ซึ่งเป็นรูปแบบข้อมูลที่ Task Transform คาดหวัง
        records = df.to_dict(orient='records')
        
        kwargs['ti'].xcom_push(key='api_records', value=records)

    except Exception as e:
        print(f"Failed to read CSV from URL: {e}")
        raise

# ---Transform---
def transform_waste_data(**kwargs):
    """แปลงข้อมูลของเสียอันตราย"""
    print("Starting data transformation...")
    
    records = kwargs['ti'].xcom_pull(key='api_records', task_ids='extract_data_from_csv')
    
    if not records:
        raise ValueError("No records received from extract task.")
        
    df = pd.DataFrame(records)
    
    # !!! ข้อควรระวัง !!!
    # คุณต้องตรวจสอบว่าชื่อคอลัมน์ในไฟล์ CSV ตรงกับ key ใน column_mapping หรือไม่
    # เช่น 'ปีงบประมาณ', 'อำเภอ', 'อปท.', 'ปริมาณของเสียอันตราย', 'หน่วย'
    # หากไม่ตรง ให้แก้ไข key ใน dictionary ด้านล่างให้ถูกต้อง
    column_mapping = {
        'ปีงบประมาณ': 'fiscal_year',
        'อำเภอ': 'district',
        'อปท.': 'local_admin_org',
        'ปริมาณของเสียอันตราย': 'waste_amount',
        'หน่วย': 'unit'
    }
    df = df.rename(columns=column_mapping)
    
    # เลือกเฉพาะคอลัมน์ที่ต้องการ (ถ้ามีอยู่ในไฟล์ CSV)
    required_cols = ['fiscal_year', 'district', 'local_admin_org', 'waste_amount', 'unit']
    df = df[required_cols]
    
    df['waste_amount'] = pd.to_numeric(df['waste_amount'], errors='coerce').fillna(0)
    
    print("Data transformed successfully. Final columns:", df.columns.tolist())
    
    output_dir = '/opt/airflow/data'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'hazardous_waste_data.csv')

    df.to_csv(output_path, index=False)
    
    kwargs['ti'].xcom_push(key='transformed_data_path', value=output_path)

# --- 3. ฟังก์ชัน Load (ไม่เปลี่ยนแปลง) ---
def load_data_to_db(**kwargs):
    """โหลดข้อมูลจากไฟล์ CSV เข้าฐานข้อมูล PostgreSQL"""
    data_path = kwargs['ti'].xcom_pull(key='transformed_data_path', task_ids='transform_data')
    
    if data_path is None:
        print("No data file path received. Skipping load task.")
        return

    db_conn = kwargs['params']
    
    df = pd.read_csv(data_path)
    df['data_ingested_at'] = datetime.now()
    
    engine = create_engine(
        f"postgresql+psycopg2://{db_conn['user']}:{db_conn['pswd']}@{db_conn['host']}:{db_conn['port']}/{db_conn['name']}"
    )
    
    table_name = 'hazardous_waste'
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    
    print(f"Success: Loaded {len(df)} records to table '{table_name}'.")

# --- 4. แก้ไข DAG Definition ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nakhonpathom_waste_csv_etl_dag',
    default_args=default_args,
    description='ETL DAG for Nakhon Pathom waste data from a CSV file.',
    schedule_interval=None, # ตั้งเป็น None เพื่อให้รันด้วยมือ (Manual Trigger)
    catchup=False,
    tags=['csv', 'nakhonpathom', 'etl']
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data_from_csv',
        python_callable=extract_csv_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_waste_data,
    )
    
    load_task = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_data_to_db,
    params={
        'host': 'postgres',
        'name': 'airflow',
        'user': 'airflow',
        'pswd': 'airflow',
        'port': 5432,
    },
    dag=dag,
)

    extract_task >> transform_task >> load_task
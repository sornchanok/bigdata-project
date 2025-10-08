from __future__ import annotations
import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os

# --- รวมขั้นตอน Extract และ Load ไว้ในฟังก์ชันเดียว ---
def read_cleansed_csv_and_load_to_db(**kwargs):
    """
    อ่านไฟล์ CSV ที่ Cleansing แล้วจากโฟลเดอร์ data 
    แล้วโหลดเข้าฐานข้อมูล PostgreSQL โดยตรง
    """
    print("Starting task: Read cleansed CSV and load to DB...")
    
    # 1. กำหนด Path ไปยังไฟล์ CSV ที่ Cleansing แล้ว
    cleansed_csv_path = "/opt/airflow/data/cleansed_waste_data.csv"
    
    try:
        # 2. อ่านไฟล์ CSV เข้า DataFrame
        if not os.path.exists(cleansed_csv_path):
            raise FileNotFoundError(f"Cleansed file not found at: {cleansed_csv_path}")

        df = pd.read_csv(cleansed_csv_path)
        print(f"Successfully read {len(df)} rows from {cleansed_csv_path}.")

        # 3. โหลดข้อมูลเข้าฐานข้อมูล
        db_conn = kwargs['params']
        df['data_ingested_at'] = datetime.now()
        
        engine = create_engine(
            f"postgresql+psycopg2://{db_conn['user']}:{db_conn['pswd']}@{db_conn['host']}:{db_conn['port']}/{db_conn['name']}"
        )
        
        table_name = 'hazardous_waste_cleansed'
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        
        print(f"Success: Loaded {len(df)} records to table '{table_name}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

# --- DAG Definition ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 4),
    'retries': 1,
}

with DAG(
    dag_id='cleansed_csv_to_db_etl_dag', # ตั้งชื่อ DAG ใหม่ให้สื่อความหมาย
    default_args=default_args,
    description='Reads a cleansed CSV from the data folder and loads it to the database.',
    schedule_interval=None,
    catchup=False,
    tags=['csv', 'local', 'load']
) as dag:
    
    # --- สร้าง Task เดียวที่ทำงานทั้งหมด ---
    process_cleansed_file_task = PythonOperator(
        task_id='read_and_load_cleansed_csv',
        python_callable=read_cleansed_csv_and_load_to_db,
        params={
            'host': 'recalls_db',   # ใช้ DB ตัวที่สองที่คุณสร้าง
            'name': 'recalls_db',
            'user': 'admin',
            'pswd': 'admin',
            'port': 5432,           # พอร์ตภายใน Container คือ 5432
        },
        dag=dag,
    )
    process_cleansed_file_task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import sys
import os

# Tambahkan path ke PYTHONPATH untuk mengimport modul custom
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

# Import helper functions
from src.utils.data_sampling import DataSampler
from src.utils.data_generator import DataGenerator

# Default arguments untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_customer_data(**context):
    """Extract data pelanggan dari file CSV"""
    try:
        # Baca data dari CSV
        df = pd.read_csv('/opt/airflow/data/processed/sampled/customer_sampled.csv')
        
        # Convert dates ke string format sebelum di-serialize
        if 'birthdate' in df.columns:
            df['birthdate'] = pd.to_datetime(df['birthdate']).dt.strftime('%Y-%m-%d')
        if 'first_join_date' in df.columns:
            df['first_join_date'] = pd.to_datetime(df['first_join_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert DataFrame ke dictionary dengan nilai yang bisa di-serialize
        df_dict = df.to_dict(orient='records')
        
        # Save ke XCOM
        context['task_instance'].xcom_push(key='customer_data', value=df_dict)
        return "Ekstraksi data customer berhasil"
    except Exception as e:
        raise Exception(f"Gagal mengekstrak data customer: {str(e)}")

def transform_customer_data(**context):
    """Transform data pelanggan"""
    # Setup logger di dalam fungsi
    logger = logging.getLogger('airflow.task')
    
    try:
        logger.info("Memulai transformasi data customer")
        
        # Ambil data dari XCOM
        df_dict = context['task_instance'].xcom_pull(key='customer_data')
        logger.info(f"Data berhasil diambil dari XCOM: {len(df_dict)} records")
        
        # Convert ke DataFrame
        df = pd.DataFrame(df_dict)
        logger.info(f"Columns in DataFrame: {df.columns.tolist()}")
        
        # Convert dates dengan error handling
        try:
            df['birthdate'] = pd.to_datetime(df['birthdate'])
            df['first_join_date'] = pd.to_datetime(df['first_join_date'])
            logger.info("Konversi tanggal berhasil")
        except Exception as e:
            logger.error(f"Error saat konversi tanggal: {str(e)}")
            raise
        
        # Hitung umur
        today = pd.Timestamp.now()
        logger.info(f"Current timestamp: {today}")
        
        df['age'] = today.year - df['birthdate'].dt.year - (
            (today.month < df['birthdate'].dt.month) | 
            ((today.month == df['birthdate'].dt.month) & (today.day < df['birthdate'].dt.day))
        )
        
        # Hitung customer tenure
        df['customer_tenure_days'] = (today - df['first_join_date']).dt.days
        
        # Kategorisasi
        df['customer_segment'] = pd.cut(
            df['customer_tenure_days'],
            bins=[-float('inf'), 90, 180, 365, float('inf')],
            labels=['New', 'Regular', 'Loyal', 'VIP']
        )
        
        # Flag mobile users
        df['is_mobile_user'] = df['device_type'].str.lower() == 'mobile'
        
        # Statistik lokasi
        location_stats = df.groupby('home_location').agg({
            'customer_id': 'count',
            'age': 'mean',
            'customer_tenure_days': 'mean'
        }).reset_index()
        
        # Convert datetime columns ke string sebelum serialisasi
        df['birthdate'] = df['birthdate'].dt.strftime('%Y-%m-%d')
        df['first_join_date'] = df['first_join_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info("Transformasi selesai, menyiapkan data untuk XCOM")
        
        # Convert DataFrame ke dict
        transformed_dict = df.to_dict(orient='records')
        location_stats_dict = location_stats.to_dict(orient='records')
        
        # Push ke XCOM
        context['task_instance'].xcom_push(
            key='transformed_customer_data', 
            value=transformed_dict
        )
        context['task_instance'].xcom_push(
            key='location_stats', 
            value=location_stats_dict
        )
        
        logger.info("Transformasi data customer berhasil")
        return "Transformasi data customer berhasil"
        
    except Exception as e:
        logger.error(f"Error dalam transform_customer_data: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        raise Exception(f"Gagal mentransformasi data customer: {str(e)}")

def load_customer_data(**context):
    """Load data pelanggan ke warehouse"""
    try:
        # Ambil data dari XCOM
        df = pd.DataFrame(context['task_instance'].xcom_pull(
            key='transformed_customer_data'))
        location_stats = pd.DataFrame(context['task_instance'].xcom_pull(
            key='location_stats'))
        
        # Inisialisasi PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Load transformed customer data
        df.to_sql(
            'dim_customers',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        # Load location statistics
        location_stats.to_sql(
            'dim_customer_locations',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        return "Load data customer ke warehouse berhasil"
    except Exception as e:
        raise Exception(f"Gagal load data customer: {str(e)}")

def analyze_customer_behavior(**context):
    """Analisis perilaku pelanggan"""
    try:
        # Ambil data customer
        df = pd.DataFrame(context['task_instance'].xcom_pull(
            key='transformed_customer_data'))
        
        # Inisialisasi PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Query data click stream untuk analisis
        click_query = """
            SELECT 
                cs.session_id,
                cs.event_name,
                cs.event_time,
                cs.traffic_source,
                t.customer_id
            FROM raw.click_stream cs
            LEFT JOIN raw.transactions t ON cs.session_id = t.session_id
            WHERE t.customer_id IS NOT NULL
        """
        click_df = pd.read_sql(click_query, pg_hook.get_sqlalchemy_engine())
        
        # Analisis basic metrics per customer
        customer_behavior = click_df.groupby('customer_id').agg({
            'session_id': 'nunique',  # Jumlah sessions
            'event_name': 'count',    # Total events
        }).reset_index()
        
        customer_behavior.columns = ['customer_id', 'total_sessions', 'total_events']
        
        # Calculate events per session
        customer_behavior['events_per_session'] = (
            customer_behavior['total_events'] / 
            customer_behavior['total_sessions']
        )
        
        # Merge dengan customer segments
        customer_behavior = customer_behavior.merge(
            df[['customer_id', 'customer_segment']],
            on='customer_id',
            how='left'
        )
        
        # Save hasil analisis
        customer_behavior.to_sql(
            'fact_customer_behavior',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        return "Analisis perilaku customer berhasil"
    except Exception as e:
        raise Exception(f"Gagal menganalisis perilaku customer: {str(e)}")

# Buat DAG
with DAG(
    'customer_analytics',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Pipeline untuk analisis data customer',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    
    # Task 1: Create tables
    create_tables = PostgresOperator(
        task_id='create_customer_tables',
        postgres_conn_id='postgres_default',
        sql="""
            -- Dimension table untuk customer
            CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
                customer_id INTEGER PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                gender VARCHAR(1),
                age INTEGER,
                device_type VARCHAR(50),
                home_location VARCHAR(100),
                customer_tenure_days INTEGER,
                customer_segment VARCHAR(50),
                is_mobile_user BOOLEAN
            );
            
            -- Dimension table untuk statistik lokasi
            CREATE TABLE IF NOT EXISTS warehouse.dim_customer_locations (
                home_location VARCHAR(100) PRIMARY KEY,
                customer_id INTEGER,
                age FLOAT,
                customer_tenure_days FLOAT
            );
            
            -- Fact table untuk perilaku customer
            CREATE TABLE IF NOT EXISTS warehouse.fact_customer_behavior (
                customer_id INTEGER,
                total_sessions INTEGER,
                total_events INTEGER,
                events_per_session FLOAT,
                customer_segment VARCHAR(50)
            );
        """
    )
    
    # Task 2: Extract customer data
    extract_customers = PythonOperator(
        task_id='extract_customer_data',
        python_callable=extract_customer_data
    )
    
    # Task 3: Transform customer data
    transform_customers = PythonOperator(
        task_id='transform_customer_data',
        python_callable=transform_customer_data
    )
    
    # Task 4: Load customer data
    load_customers = PythonOperator(
        task_id='load_customer_data',
        python_callable=load_customer_data
    )
    
    # Task 5: Analyze customer behavior
    analyze_customers = PythonOperator(
        task_id='analyze_customer_behavior',
        python_callable=analyze_customer_behavior
    )
    
    # Set dependencies
    create_tables >> extract_customers >> transform_customers >> load_customers >> analyze_customers
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
import numpy as np
from typing import Dict, List
import sys
import os

# Tambahkan path ke PYTHONPATH untuk mengimport modul custom
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

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

def extract_transaction_data(**context):
    """Extract data transaksi dari file CSV"""
    try:
        # Baca data transaksi
        df = pd.read_csv('/opt/airflow/data/processed/sampled/transactions_sampled.csv')
        
        # Convert timestamps
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['shipment_date_limit'] = pd.to_datetime(df['shipment_date_limit'])
        
        # Parse product metadata JSON
        df['product_metadata'] = df['product_metadata'].apply(json.loads)
        
        # Save ke XCOM
        context['task_instance'].xcom_push(key='transaction_data', value=df.to_dict())
        return "Ekstraksi data transaksi berhasil"
    except Exception as e:
        raise Exception(f"Gagal mengekstrak data transaksi: {str(e)}")

def transform_transaction_data(**context):
    """Transform data transaksi"""
    try:
        # Ambil data dari XCOM
        df = pd.DataFrame(context['task_instance'].xcom_pull(key='transaction_data'))
        
        # Extract informasi dari product_metadata
        def extract_metadata(row):
            products = row['product_metadata']
            total_items = sum(item['quantity'] for item in products)
            total_product_value = sum(item['quantity'] * item['item_price'] for item in products)
            return pd.Series({
                'total_items': total_items,
                'total_product_value': total_product_value,
                'num_unique_products': len(products)
            })

        # Apply metadata extraction
        metadata_df = df.apply(extract_metadata, axis=1)
        df = pd.concat([df, metadata_df], axis=1)
        
        # Calculate metrics
        df['shipping_cost_ratio'] = (df['shipment_fee'] / df['total_amount']) * 100
        df['discount_ratio'] = (df['promo_amount'] / df['total_amount']) * 100
        
        # Add temporal features
        df['order_hour'] = df['created_at'].dt.hour
        df['order_day'] = df['created_at'].dt.day_name()
        df['order_month'] = df['created_at'].dt.month
        df['order_year'] = df['created_at'].dt.year
        
        # Categorize transactions
        df['transaction_size'] = pd.qcut(
            df['total_amount'], 
            q=4, 
            labels=['Small', 'Medium', 'Large', 'Extra Large']
        )
        
        # Save transformed data
        context['task_instance'].xcom_push(
            key='transformed_transaction_data', 
            value=df.to_dict()
        )
        return "Transformasi data transaksi berhasil"
    except Exception as e:
        raise Exception(f"Gagal mentransformasi data transaksi: {str(e)}")

def analyze_sales_performance(**context):
    """Analyze sales performance metrics"""
    try:
        # Get transformed data
        df = pd.DataFrame(context['task_instance'].xcom_pull(
            key='transformed_transaction_data'
        ))
        
        # Daily sales analysis
        daily_sales = df.groupby(df['created_at'].dt.date).agg({
            'booking_id': 'count',
            'total_amount': 'sum',
            'total_items': 'sum',
            'promo_amount': 'sum'
        }).reset_index()
        daily_sales.columns = ['date', 'num_transactions', 'total_sales', 
                             'total_items', 'total_discount']
        
        # Payment method analysis
        payment_analysis = df.groupby('payment_method').agg({
            'booking_id': 'count',
            'total_amount': 'sum',
            'total_items': 'sum'
        }).reset_index()
        
        # Promo code effectiveness
        promo_analysis = df[df['promo_code'].notna()].groupby('promo_code').agg({
            'booking_id': 'count',
            'promo_amount': 'sum',
            'total_amount': 'sum'
        }).reset_index()
        promo_analysis['avg_discount_ratio'] = (
            promo_analysis['promo_amount'] / promo_analysis['total_amount'] * 100
        )
        
        # Initialize PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Save analyses to warehouse
        daily_sales.to_sql(
            'fact_daily_sales',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        payment_analysis.to_sql(
            'fact_payment_analysis',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        promo_analysis.to_sql(
            'fact_promo_effectiveness',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        return "Analisis performa penjualan berhasil"
    except Exception as e:
        raise Exception(f"Gagal menganalisis performa penjualan: {str(e)}")

def calculate_customer_ltv(**context):
    """Calculate Customer Lifetime Value"""
    try:
        # Get transaction data
        df = pd.DataFrame(context['task_instance'].xcom_pull(
            key='transformed_transaction_data'
        ))
        
        # Calculate basic LTV metrics per customer
        customer_ltv = df.groupby('customer_id').agg({
            'booking_id': 'count',  # frequency
            'total_amount': ['sum', 'mean'],  # monetary
            'created_at': ['min', 'max']  # recency
        }).reset_index()
        
        # Flatten column names
        customer_ltv.columns = [
            'customer_id', 'purchase_frequency', 'total_spent', 
            'avg_order_value', 'first_purchase', 'last_purchase'
        ]
        
        # Calculate days between first and last purchase
        customer_ltv['customer_lifetime'] = (
            pd.to_datetime(customer_ltv['last_purchase']) - 
            pd.to_datetime(customer_ltv['first_purchase'])
        ).dt.days
        
        # Calculate purchase rate (purchases per day)
        customer_ltv['purchase_rate'] = (
            customer_ltv['purchase_frequency'] / 
            customer_ltv['customer_lifetime'].replace(0, 1)
        )
        
        # Calculate LTV
        customer_ltv['customer_ltv'] = (
            customer_ltv['avg_order_value'] * 
            customer_ltv['purchase_rate'] * 365
        )  # Projected annual value
        
        # Initialize PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Save to warehouse
        customer_ltv.to_sql(
            'fact_customer_ltv',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        return "Perhitungan Customer LTV berhasil"
    except Exception as e:
        raise Exception(f"Gagal menghitung Customer LTV: {str(e)}")

# Create DAG
with DAG(
    'sales_analytics',
    default_args=default_args,
    description='Pipeline untuk analisis penjualan',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    
    # Task 1: Create tables
    create_tables = PostgresOperator(
        task_id='create_sales_tables',
        postgres_conn_id='postgres_default',
        sql="""
            -- Fact table untuk daily sales
            CREATE TABLE IF NOT EXISTS warehouse.fact_daily_sales (
                date DATE,
                num_transactions INTEGER,
                total_sales DECIMAL,
                total_items INTEGER,
                total_discount DECIMAL
            );
            
            -- Fact table untuk payment analysis
            CREATE TABLE IF NOT EXISTS warehouse.fact_payment_analysis (
                payment_method VARCHAR(50),
                booking_id INTEGER,
                total_amount DECIMAL,
                total_items INTEGER
            );
            
            -- Fact table untuk promo effectiveness
            CREATE TABLE IF NOT EXISTS warehouse.fact_promo_effectiveness (
                promo_code VARCHAR(50),
                booking_id INTEGER,
                promo_amount DECIMAL,
                total_amount DECIMAL,
                avg_discount_ratio DECIMAL
            );
            
            -- Fact table untuk customer LTV
            CREATE TABLE IF NOT EXISTS warehouse.fact_customer_ltv (
                customer_id INTEGER PRIMARY KEY,
                purchase_frequency INTEGER,
                total_spent DECIMAL,
                avg_order_value DECIMAL,
                first_purchase TIMESTAMP,
                last_purchase TIMESTAMP,
                customer_lifetime INTEGER,
                purchase_rate DECIMAL,
                customer_ltv DECIMAL
            );
        """
    )
    
    # Task 2: Extract transaction data
    extract_transactions = PythonOperator(
        task_id='extract_transaction_data',
        python_callable=extract_transaction_data
    )
    
    # Task 3: Transform transaction data
    transform_transactions = PythonOperator(
        task_id='transform_transaction_data',
        python_callable=transform_transaction_data
    )
    
    # Task 4: Analyze sales performance
    analyze_sales = PythonOperator(
        task_id='analyze_sales_performance',
        python_callable=analyze_sales_performance
    )
    
    # Task 5: Calculate customer LTV
    calculate_ltv = PythonOperator(
        task_id='calculate_customer_ltv',
        python_callable=calculate_customer_ltv  # Diubah dari calculate_ltv
    )
    
    # Set dependencies
    create_tables >> extract_transactions >> transform_transactions >> [analyze_sales, calculate_ltv]
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

def extract_product_data(**context):
    """Extract data produk dari file CSV"""
    try:
        # Baca data produk
        df_products = pd.read_csv('/opt/airflow/data/processed/sampled/product_sampled.csv')
        
        # Baca data transaksi untuk analisis penjualan produk
        df_transactions = pd.read_csv('/opt/airflow/data/processed/sampled/transactions_sampled.csv')
        df_transactions['created_at'] = pd.to_datetime(df_transactions['created_at'])
        df_transactions['product_metadata'] = df_transactions['product_metadata'].apply(json.loads)
        
        # Save ke XCOM
        context['task_instance'].xcom_push(key='product_data', value=df_products.to_dict())
        context['task_instance'].xcom_push(key='transaction_data', value=df_transactions.to_dict())
        return "Ekstraksi data produk berhasil"
    except Exception as e:
        raise Exception(f"Gagal mengekstrak data produk: {str(e)}")

def transform_product_data(**context):
    """Transform data produk dan hitung metrik dasar"""
    try:
        # Ambil data dari XCOM
        df_products = pd.DataFrame(context['task_instance'].xcom_pull(key='product_data'))
        df_transactions = pd.DataFrame(context['task_instance'].xcom_pull(key='transaction_data'))
        
        # Extract product sales dari metadata
        product_sales = []
        for _, row in df_transactions.iterrows():
            for item in row['product_metadata']:
                product_sales.append({
                    'transaction_date': row['created_at'],
                    'product_id': item['product_id'],
                    'quantity': item['quantity'],
                    'item_price': item['item_price'],
                    'total_value': item['quantity'] * item['item_price']
                })
        
        df_sales = pd.DataFrame(product_sales)
        df_sales['transaction_date'] = pd.to_datetime(df_sales['transaction_date'])
        
        # Merge product info dengan sales
        df_product_analysis = df_products.merge(
            df_sales.groupby('product_id').agg({
                'quantity': 'sum',
                'total_value': 'sum',
                'item_price': 'mean'
            }).reset_index(),
            left_on='id',
            right_on='product_id',
            how='left'
        )
        
        # Fill NaN values untuk produk yang belum terjual
        df_product_analysis = df_product_analysis.fillna({
            'quantity': 0,
            'total_value': 0,
            'item_price': 0
        })
        
        # Save hasil transformasi
        context['task_instance'].xcom_push(
            key='transformed_product_data',
            value=df_product_analysis.to_dict()
        )
        context['task_instance'].xcom_push(
            key='product_sales_data',
            value=df_sales.to_dict()
        )
        return "Transformasi data produk berhasil"
    except Exception as e:
        raise Exception(f"Gagal mentransformasi data produk: {str(e)}")

def analyze_product_performance(**context):
    """Analisis performa produk"""
    try:
        # Ambil data yang sudah ditransformasi
        df_products = pd.DataFrame(context['task_instance'].xcom_pull(
            key='transformed_product_data'
        ))
        df_sales = pd.DataFrame(context['task_instance'].xcom_pull(
            key='product_sales_data'
        ))
        
        # 1. Category Performance Analysis
        category_performance = df_products.groupby('masterCategory').agg({
            'id': 'count',                  # jumlah produk
            'quantity': 'sum',              # total terjual
            'total_value': 'sum'            # total nilai penjualan
        }).reset_index()
        
        category_performance.columns = [
            'master_category', 'product_count', 
            'total_quantity_sold', 'total_sales_value'
        ]
        
        # 2. Seasonal Analysis
        seasonal_performance = df_products.groupby(['season', 'masterCategory']).agg({
            'quantity': 'sum',
            'total_value': 'sum'
        }).reset_index()
        
        # 3. Price Range Analysis
        df_products['price_range'] = pd.qcut(
            df_products['item_price'],
            q=4,
            labels=['Budget', 'Medium', 'Premium', 'Luxury']
        )
        
        price_performance = df_products.groupby('price_range').agg({
            'id': 'count',
            'quantity': 'sum',
            'total_value': 'sum'
        }).reset_index()
        
        # 4. Top Products Analysis
        top_products = df_products.nlargest(100, 'total_value')[
            ['id', 'productDisplayName', 'masterCategory', 
             'quantity', 'total_value', 'item_price']
        ]
        
        # Initialize PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Save analyses to warehouse
        category_performance.to_sql(
            'fact_category_performance',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        seasonal_performance.to_sql(
            'fact_seasonal_performance',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        price_performance.to_sql(
            'fact_price_performance',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        top_products.to_sql(
            'fact_top_products',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        return "Analisis performa produk berhasil"
    except Exception as e:
        raise Exception(f"Gagal menganalisis performa produk: {str(e)}")

def analyze_product_combinations(**context):
    """Analisis kombinasi produk yang sering dibeli bersamaan"""
    try:
        # Ambil data transaksi
        df_transactions = pd.DataFrame(context['task_instance'].xcom_pull(
            key='transaction_data'
        ))
        
        # Create list of product combinations per transaction
        product_combinations = []
        for _, row in df_transactions.iterrows():
            products = [item['product_id'] for item in row['product_metadata']]
            if len(products) > 1:
                # Get all possible pairs of products
                for i in range(len(products)):
                    for j in range(i+1, len(products)):
                        product_combinations.append({
                            'product1': min(products[i], products[j]),
                            'product2': max(products[i], products[j])
                        })
        
        if product_combinations:
            df_combinations = pd.DataFrame(product_combinations)
            
            # Count frequency of each combination
            combination_freq = df_combinations.groupby(
                ['product1', 'product2']
            ).size().reset_index(name='frequency')
            
            # Get product names
            df_products = pd.DataFrame(context['task_instance'].xcom_pull(
                key='product_data'
            ))
            product_names = df_products[['id', 'productDisplayName']]
            
            # Add product names to combinations
            combination_freq = combination_freq.merge(
                product_names,
                left_on='product1',
                right_on='id',
                how='left'
            ).merge(
                product_names,
                left_on='product2',
                right_on='id',
                how='left',
                suffixes=('_1', '_2')
            )
            
            # Save to warehouse
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            combination_freq.to_sql(
                'fact_product_combinations',
                pg_hook.get_sqlalchemy_engine(),
                schema='warehouse',
                if_exists='replace',
                index=False
            )
        
        return "Analisis kombinasi produk berhasil"
    except Exception as e:
        raise Exception(f"Gagal menganalisis kombinasi produk: {str(e)}")

# Create DAG
with DAG(
    'product_analytics',
    default_args=default_args,
    description='Pipeline untuk analisis produk',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    
    # Task 1: Create tables
    create_tables = PostgresOperator(
        task_id='create_product_tables',
        postgres_conn_id='postgres_default',
        sql="""
            -- Fact table untuk category performance
            CREATE TABLE IF NOT EXISTS warehouse.fact_category_performance (
                master_category VARCHAR(100),
                product_count INTEGER,
                total_quantity_sold INTEGER,
                total_sales_value DECIMAL
            );
            
            -- Fact table untuk seasonal performance
            CREATE TABLE IF NOT EXISTS warehouse.fact_seasonal_performance (
                season VARCHAR(50),
                master_category VARCHAR(100),
                quantity INTEGER,
                total_value DECIMAL
            );
            
            -- Fact table untuk price performance
            CREATE TABLE IF NOT EXISTS warehouse.fact_price_performance (
                price_range VARCHAR(50),
                id INTEGER,
                quantity INTEGER,
                total_value DECIMAL
            );
            
            -- Fact table untuk top products
            CREATE TABLE IF NOT EXISTS warehouse.fact_top_products (
                id INTEGER,
                product_display_name VARCHAR(255),
                master_category VARCHAR(100),
                quantity INTEGER,
                total_value DECIMAL,
                item_price DECIMAL
            );
            
            -- Fact table untuk product combinations
            CREATE TABLE IF NOT EXISTS warehouse.fact_product_combinations (
                product1 INTEGER,
                product2 INTEGER,
                product_display_name_1 VARCHAR(255),
                product_display_name_2 VARCHAR(255),
                frequency INTEGER
            );
        """
    )
    
    # Task 2: Extract product data
    extract_products = PythonOperator(
        task_id='extract_product_data',
        python_callable=extract_product_data
    )
    
    # Task 3: Transform product data
    transform_products = PythonOperator(
        task_id='transform_product_data',
        python_callable=transform_product_data
    )
    
    # Task 4: Analyze product performance
    analyze_products = PythonOperator(
        task_id='analyze_product_performance',
        python_callable=analyze_product_performance
    )
    
    # Task 5: Analyze product combinations
    analyze_combinations = PythonOperator(
        task_id='analyze_product_combinations',
        python_callable=analyze_product_combinations
    )
    
    # Set dependencies
    create_tables >> extract_products >> transform_products >> [analyze_products, analyze_combinations]
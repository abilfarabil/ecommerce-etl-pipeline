from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
import json
import sys
import os

# Tambahkan path ke PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def setup_selenium():
    """Setup Selenium WebDriver dengan Chrome dalam mode headless"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def extract_product_info(**context):
    """Extract data produk untuk perbandingan"""
    try:
        # Baca data produk
        df_products = pd.read_csv('/opt/airflow/data/processed/sampled/product_sampled.csv')
        
        # Pilih sampel produk untuk perbandingan (20% dari total)
        sample_size = int(len(df_products) * 0.2)
        sampled_products = df_products.sample(n=sample_size, random_state=42)
        
        # Save ke XCOM
        context['task_instance'].xcom_push(
            key='products_to_compare',
            value=sampled_products.to_dict()
        )
        return "Ekstraksi data produk untuk perbandingan berhasil"
    except Exception as e:
        raise Exception(f"Gagal mengekstrak data produk: {str(e)}")

def scrape_tokopedia_prices(**context):
    """Scrape harga produk dari Tokopedia"""
    try:
        # Ambil data produk dari XCOM
        df_products = pd.DataFrame(context['task_instance'].xcom_pull(
            key='products_to_compare'
        ))
        
        scraped_data = []
        driver = setup_selenium()
        
        for _, product in df_products.iterrows():
            try:
                # Buat search query
                search_query = f"{product['productDisplayName']} {product['masterCategory']}"
                encoded_query = search_query.replace(' ', '%20')
                url = f"https://www.tokopedia.com/search?q={encoded_query}"
                
                # Load halaman
                driver.get(url)
                time.sleep(random.uniform(2, 4))  # Random delay
                
                # Tunggu sampai product cards muncul
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='master-product-card']"))
                )
                
                # Parse halaman
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                product_cards = soup.find_all("div", {"data-testid": "master-product-card"})
                
                # Ambil 5 produk pertama
                for idx, card in enumerate(product_cards[:5]):
                    try:
                        # Extract informasi
                        title = card.find("span", {"class": "css-1bjwylw"}).text
                        price = card.find("span", {"class": "css-o5uqvq"}).text
                        price = int(price.replace("Rp", "").replace(".", ""))
                        seller = card.find("span", {"class": "css-1kdc32b"}).text
                        
                        scraped_data.append({
                            'our_product_id': product['id'],
                            'our_product_name': product['productDisplayName'],
                            'our_category': product['masterCategory'],
                            'competitor_name': seller,
                            'competitor_product': title,
                            'competitor_price': price,
                            'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                            'rank': idx + 1
                        })
                    except Exception as e:
                        continue
                
                # Random delay antara produk
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                continue
        
        driver.quit()
        
        # Convert ke DataFrame
        df_scraped = pd.DataFrame(scraped_data)
        
        # Save ke XCOM
        context['task_instance'].xcom_push(
            key='scraped_data',
            value=df_scraped.to_dict()
        )
        
        return "Web scraping berhasil"
    except Exception as e:
        raise Exception(f"Gagal melakukan web scraping: {str(e)}")

def analyze_competitor_prices(**context):
    """Analisis perbandingan harga dengan kompetitor"""
    try:
        # Ambil data dari XCOM
        df_scraped = pd.DataFrame(context['task_instance'].xcom_pull(
            key='scraped_data'
        ))
        df_products = pd.DataFrame(context['task_instance'].xcom_pull(
            key='products_to_compare'
        ))
        
        # Hitung statistik harga kompetitor
        price_analysis = df_scraped.groupby('our_product_id').agg({
            'competitor_price': ['mean', 'min', 'max', 'std']
        }).reset_index()
        
        price_analysis.columns = [
            'product_id', 'avg_competitor_price', 
            'min_competitor_price', 'max_competitor_price',
            'std_competitor_price'
        ]
        
        # Merge dengan data produk kita
        price_comparison = df_products.merge(
            price_analysis,
            left_on='id',
            right_on='product_id',
            how='left'
        )
        
        # Hitung price difference
        price_comparison['price_difference'] = (
            price_comparison['item_price'] - 
            price_comparison['avg_competitor_price']
        )
        price_comparison['price_difference_percentage'] = (
            price_comparison['price_difference'] / 
            price_comparison['avg_competitor_price'] * 100
        )
        
        # Kategorisasi posisi harga
        price_comparison['price_position'] = np.where(
            price_comparison['price_difference'] > 0,
            'Above Market',
            np.where(
                price_comparison['price_difference'] < 0,
                'Below Market',
                'At Market'
            )
        )
        
        # Save analyses ke warehouse
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Save raw scraped data
        df_scraped.to_sql(
            'fact_competitor_prices',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        # Save price comparison analysis
        price_comparison.to_sql(
            'fact_price_comparison',
            pg_hook.get_sqlalchemy_engine(),
            schema='warehouse',
            if_exists='replace',
            index=False
        )
        
        return "Analisis perbandingan harga berhasil"
    except Exception as e:
        raise Exception(f"Gagal menganalisis perbandingan harga: {str(e)}")

# Create DAG
with DAG(
    'web_scraping',
    default_args=default_args,
    description='Pipeline untuk web scraping dan analisis kompetitor',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    
    # Task 1: Create tables
    create_tables = PostgresOperator(
        task_id='create_competitor_tables',
        postgres_conn_id='postgres_default',
        sql="""
            -- Fact table untuk raw competitor prices
            CREATE TABLE IF NOT EXISTS warehouse.fact_competitor_prices (
                our_product_id INTEGER,
                our_product_name VARCHAR(255),
                our_category VARCHAR(100),
                competitor_name VARCHAR(255),
                competitor_product VARCHAR(255),
                competitor_price DECIMAL,
                scrape_date DATE,
                rank INTEGER
            );
            
            -- Fact table untuk price comparison
            CREATE TABLE IF NOT EXISTS warehouse.fact_price_comparison (
                product_id INTEGER,
                productDisplayName VARCHAR(255),
                masterCategory VARCHAR(100),
                item_price DECIMAL,
                avg_competitor_price DECIMAL,
                min_competitor_price DECIMAL,
                max_competitor_price DECIMAL,
                std_competitor_price DECIMAL,
                price_difference DECIMAL,
                price_difference_percentage DECIMAL,
                price_position VARCHAR(50)
            );
        """
    )
    
    # Task 2: Extract product info
    extract_products = PythonOperator(
        task_id='extract_product_info',
        python_callable=extract_product_info
    )
    
    # Task 3: Scrape competitor prices
    scrape_prices = PythonOperator(
        task_id='scrape_tokopedia_prices',
        python_callable=scrape_tokopedia_prices
    )
    
    # Task 4: Analyze competitor prices
    analyze_prices = PythonOperator(
        task_id='analyze_competitor_prices',
        python_callable=analyze_competitor_prices
    )
    
    # Set dependencies
    create_tables >> extract_products >> scrape_prices >> analyze_prices
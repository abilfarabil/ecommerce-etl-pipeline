# src/load/loaders.py

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import Dict, List
from datetime import datetime
import os
from dotenv import load_dotenv
from decimal import Decimal
import pandas.api.types

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgresLoader:
    """Class untuk loading data ke PostgreSQL"""
    
    def __init__(self, is_docker: bool = False):
        """
        Initialize connection ke PostgreSQL
        
        Args:
            is_docker: Boolean yang menandakan apakah running di docker atau tidak
        """
        # Karena kita menjalankan dari host machine, selalu gunakan localhost
        self.conn_params = {
            'dbname': os.getenv('POSTGRES_DB', 'ecommerce_dw'),
            'user': os.getenv('POSTGRES_USER', 'admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'admin123'),
            'host': 'localhost',  # Selalu gunakan localhost karena kita expose port 5432
            'port': '5432'
        }
        
        logger.info(f"Initializing PostgresLoader with host: {self.conn_params['host']}")
        
        # Test connection
        self.test_connection()
    
    def test_connection(self):
        """Test koneksi ke database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            logger.info("Database connection successful")
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise

    def get_connection(self):
        """Create dan return connection ke PostgreSQL"""
        try:
            return psycopg2.connect(**self.conn_params)
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
    
    def create_schema(self):
        """Create schema dan tables"""
        create_tables_sql = """
        -- Create schema
        CREATE SCHEMA IF NOT EXISTS ecommerce;
        
        -- Dimension tables
        CREATE TABLE IF NOT EXISTS ecommerce.dim_customer (
            customer_id BIGINT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255),
            gender CHAR(1),
            home_location VARCHAR(100),
            home_country VARCHAR(100),
            first_join_date TIMESTAMP,
            customer_since_days INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS ecommerce.dim_product (
            product_id INTEGER PRIMARY KEY,
            master_category VARCHAR(50),
            sub_category VARCHAR(50),
            article_type VARCHAR(100),
            base_colour VARCHAR(50),
            season VARCHAR(20),
            year INTEGER,
            usage VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Fact table
        CREATE TABLE IF NOT EXISTS ecommerce.fact_sales (
            sale_id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE,
            customer_id BIGINT REFERENCES ecommerce.dim_customer(customer_id),
            booking_id VARCHAR(100),
            session_id VARCHAR(100),
            payment_method VARCHAR(50),
            payment_status VARCHAR(20),
            promo_amount DECIMAL(15,2),
            promo_code VARCHAR(50),
            shipment_fee DECIMAL(15,2),
            shipment_date_limit TIMESTAMP WITH TIME ZONE,
            shipment_location_lat DECIMAL(10,6),
            shipment_location_long DECIMAL(10,6),
            total_amount DECIMAL(15,2),
            product_id INTEGER REFERENCES ecommerce.dim_product(product_id),
            quantity INTEGER,
            item_price DECIMAL(15,2),
            sale_date DATE,
            sale_month VARCHAR(7),
            sale_quarter VARCHAR(6),
            sale_year INTEGER,
            created_at_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Drop existing indexes if they exist
        DROP INDEX IF EXISTS ecommerce.idx_customer_email;
        DROP INDEX IF EXISTS ecommerce.idx_product_category;
        DROP INDEX IF EXISTS ecommerce.idx_sales_date;
        DROP INDEX IF EXISTS ecommerce.idx_sales_customer;
        DROP INDEX IF EXISTS ecommerce.idx_sales_product;
        DROP INDEX IF EXISTS ecommerce.idx_sales_payment_method;
        DROP INDEX IF EXISTS ecommerce.idx_sales_payment_status;
        DROP INDEX IF EXISTS ecommerce.idx_product_season;
        DROP INDEX IF EXISTS ecommerce.idx_product_year;
        
        -- Create indexes for better query performance
        CREATE INDEX idx_customer_email ON ecommerce.dim_customer(email);
        CREATE INDEX idx_product_category ON ecommerce.dim_product(master_category, sub_category);
        CREATE INDEX idx_sales_date ON ecommerce.fact_sales(sale_date);
        CREATE INDEX idx_sales_customer ON ecommerce.fact_sales(customer_id);
        CREATE INDEX idx_sales_product ON ecommerce.fact_sales(product_id);
        
        -- Create indexes for common queries
        CREATE INDEX idx_sales_payment_method ON ecommerce.fact_sales(payment_method);
        CREATE INDEX idx_sales_payment_status ON ecommerce.fact_sales(payment_status);
        CREATE INDEX idx_product_season ON ecommerce.dim_product(season);
        CREATE INDEX idx_product_year ON ecommerce.dim_product(year);
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_tables_sql)
                conn.commit()
            logger.info("Schema and tables created successfully")
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            raise
    
    def load_dimension_table(self, df: pd.DataFrame, table_name: str, columns: List[str], primary_key: str = 'customer_id'):
        try:
            # Validate columns
            missing_columns = [col for col in columns if col not in df.columns]
            if missing_columns:
                raise KeyError(f"Missing columns in {table_name}: {missing_columns}")

            # Konversi tipe data untuk product_id
            if table_name == 'dim_product' and 'product_id' in df.columns:
                logger.info(f"Converting product_id type from {df['product_id'].dtype}")
                try:
                    # Konversi ke int32
                    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce').astype('int32')
                    # Validasi range
                    if (df['product_id'] < -2147483648).any() or (df['product_id'] > 2147483647).any():
                        raise ValueError("product_id values outside INTEGER range")
                    logger.info(f"Product ID conversion completed. New type: {df['product_id'].dtype}")
                except Exception as e:
                    logger.error(f"Error converting product_id: {str(e)}")
                    raise
    
                # Drop rows with None values if any
                df = df.dropna(subset=['product_id'])

            # Log data analysis
            logger.info(f"\nAnalisis data untuk {table_name}:")
            for col in columns:
                try:
                    min_val = df[col].min()
                    max_val = df[col].max()
                    dtype = df[col].dtype
                    logger.info(f"Kolom {col}:")
                    logger.info(f"  - Tipe data: {dtype}")
                    logger.info(f"  - Nilai min: {min_val}")
                    logger.info(f"  - Nilai max: {max_val}")
                except Exception as e:
                    logger.warning(f"Tidak bisa menganalisis kolom {col}: {str(e)}")

            # Load data in chunks
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    chunk_size = 1000  # Kurangi ukuran chunk
                    for i in range(0, len(df), chunk_size):
                        chunk_df = df.iloc[i:i + chunk_size]
    
                        # Tambahkan logging untuk chunk ke-22
                        if i//chunk_size + 1 == 22 and table_name == 'dim_product':
                            logger.info(f"Values in chunk 22 (product_id):")
                            logger.info(chunk_df['product_id'].values)
    
                        values = [tuple(x) for x in chunk_df[columns].values]
            
                        # Tambahkan logging untuk tipe data values
                        if i//chunk_size + 1 == 22 and table_name == 'dim_product':
                            sample_value = values[0][0]  # Ambil nilai product_id pertama
                            logger.info(f"Sample product_id type in values: {type(sample_value)}")
                            logger.info(f"Sample product_id value: {sample_value}")
                    
                        # Generate SQL
                        insert_sql = f"""
                        INSERT INTO ecommerce.{table_name} ({','.join(columns)})
                        VALUES %s
                        ON CONFLICT ({primary_key}) DO UPDATE SET
                        {','.join([f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key])},
                        updated_at = CURRENT_TIMESTAMP
                        """
                    
                        try:
                            execute_values(cur, insert_sql, values)
                            conn.commit()
                            logger.info(f"Loaded chunk {i//chunk_size + 1} into {table_name}")
                        except Exception as e:
                            logger.error(f"Error loading chunk {i//chunk_size + 1}: {str(e)}")
                            conn.rollback()
                            raise

            logger.info(f"Loaded total {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error loading {table_name}: {str(e)}")
            raise
    
    def load_fact_table(self, df: pd.DataFrame):
        """Load fact table ke PostgreSQL"""
        try:
            # Parse product metadata
            metadata_df = self.parse_product_metadata(df)
            logger.info(f"Parsed {len(metadata_df)} rows from product metadata")
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Load in chunks untuk menghindari memory issues
                    chunk_size = 5000
                    total_chunks = (len(metadata_df) + chunk_size - 1) // chunk_size
                    
                    for i in range(0, len(metadata_df), chunk_size):
                        chunk = metadata_df.iloc[i:i + chunk_size]
                        values = [tuple(x) for x in chunk.values]
                        
                        insert_sql = """
                        INSERT INTO ecommerce.fact_sales (
                            created_at, customer_id, booking_id, session_id,
                            payment_method, payment_status, promo_amount,
                            promo_code, shipment_fee, shipment_date_limit,
                            shipment_location_lat, shipment_location_long,
                            total_amount, product_id, quantity, item_price,
                            sale_date, sale_month, sale_quarter, sale_year
                        ) VALUES %s
                        """
                        
                        execute_values(cur, insert_sql, values)
                        conn.commit()
                        logger.info(f"Loaded chunk {i//chunk_size + 1}/{total_chunks} into fact_sales")
            
            logger.info(f"Loaded total {len(metadata_df)} rows into fact_sales")
        except Exception as e:
            logger.error(f"Error loading fact_sales: {str(e)}")
            raise
    
    def parse_product_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse product metadata yang sudah dalam format kolom terpisah"""
        try:
            result_df = pd.DataFrame()
        
            # Copy kolom-kolom yang dibutuhkan
            base_columns = [
                'created_at', 'customer_id', 'booking_id', 'session_id',
                'payment_method', 'payment_status', 'promo_amount', 'promo_code',
                'shipment_fee', 'shipment_date_limit', 'shipment_location_lat',
                'shipment_location_long', 'total_amount', 'sale_date',
                'sale_month', 'sale_quarter', 'sale_year'
            ]
        
            result_df = df[base_columns].copy()
        
            # Tambahkan kolom dari metadata yang sudah di-parse
            result_df['product_id'] = df['product_metadata_product_id']
            result_df['quantity'] = df['product_metadata_quantity']
            result_df['item_price'] = df['product_metadata_item_price']
        
            # Convert types
            result_df['product_id'] = pd.to_numeric(result_df['product_id'], errors='coerce').astype('Int32')
            result_df['quantity'] = pd.to_numeric(result_df['quantity'], errors='coerce').astype('Int32')
            result_df['item_price'] = pd.to_numeric(result_df['item_price'], errors='coerce')
        
            # Remove rows with invalid product_id
            result_df = result_df.dropna(subset=['product_id'])
        
            logger.info(f"Processed {len(result_df)} valid sales records")
            return result_df
        
        except Exception as e:
            logger.error(f"Error parsing product metadata: {str(e)}")
            raise

    def load_all(self, data: Dict[str, pd.DataFrame]):
        """Load semua data ke PostgreSQL"""
        try:
            # Create schema dan tables
            self.create_schema()
            
            # Load customer dimension
            self.load_dimension_table(
                data['dim_customer'],
                'dim_customer',
                ['customer_id', 'first_name', 'last_name', 'email', 'gender',
                 'home_location', 'home_country', 'first_join_date', 'customer_since_days'],
                'customer_id'
            )
            
            # Load product dimension
            self.load_dimension_table(
                data['dim_product'],
                'dim_product',
                ['product_id', 'master_category', 'sub_category', 'article_type',
                 'base_colour', 'season', 'year', 'usage'],
                'product_id'
            )
            
            # Load facts
            self.load_fact_table(data['fact_sales'])
            
            # Validate loaded data
            self.validate_data_load()
            
            logger.info("All data loaded successfully")
        except Exception as e:
            logger.error(f"Error in load_all: {str(e)}")
            raise

    def validate_data_load(self):
        """Validate data yang sudah diload"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check row counts
                    cur.execute("""
                        SELECT
                            (SELECT COUNT(*) FROM ecommerce.dim_customer) as customer_count,
                            (SELECT COUNT(*) FROM ecommerce.dim_product) as product_count,
                            (SELECT COUNT(*) FROM ecommerce.fact_sales) as sales_count
                    """)
                    counts = cur.fetchone()
                    
                    logger.info(f"""
                    Data Load Summary:
                    - Customer dimension: {counts[0]} rows
                    - Product dimension: {counts[1]} rows
                    - Sales fact: {counts[2]} rows
                    """)
                    
                    # Check for referential integrity
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM ecommerce.fact_sales f
                        LEFT JOIN ecommerce.dim_customer c ON f.customer_id = c.customer_id
                        LEFT JOIN ecommerce.dim_product p ON f.product_id = p.product_id
                        WHERE c.customer_id IS NULL OR p.product_id IS NULL
                    """)
                    orphan_records = cur.fetchone()[0]
                    
                    if orphan_records > 0:
                        logger.warning(f"Found {orphan_records} sales records with missing dimension references")
                    else:
                        logger.info("All referential integrity checks passed")
                    
            return True
        except Exception as e:
            logger.error(f"Error validating data load: {str(e)}")
            return False
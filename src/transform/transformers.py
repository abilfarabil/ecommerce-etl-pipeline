# src/transform/transformers.py

import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataTransformer:
    """Class untuk menghandle proses transformasi data"""
    
    def __init__(self, data: Dict[str, pd.DataFrame]):
        """
        Inisialisasi DataTransformer
        
        Args:
            data: Dictionary berisi DataFrame yang akan ditransformasi
        """
        self.data = data
    
    def transform_timestamps(self) -> None:
        """Transform kolom timestamp ke format yang sesuai"""
        try:
            # Transform click stream timestamps
            self.data['click_stream']['event_time'] = pd.to_datetime(
                self.data['click_stream']['event_time']
            )
            
            # Transform transaction timestamps
            self.data['transactions']['created_at'] = pd.to_datetime(
                self.data['transactions']['created_at']
            )
            self.data['transactions']['shipment_date_limit'] = pd.to_datetime(
                self.data['transactions']['shipment_date_limit']
            )
            
            # Transform customer join date
            self.data['customer']['first_join_date'] = pd.to_datetime(
                self.data['customer']['first_join_date']
            )
            
            logger.info("Timestamp transformation completed")
        except Exception as e:
            logger.error(f"Error transforming timestamps: {str(e)}")
            raise
    
    def clean_customer_data(self) -> None:
        """Clean dan standardize data customer"""
        try:
            df = self.data['customer']
            
            # Standardize gender values
            df['gender'] = df['gender'].str.upper()
            
            # Clean location data
            df['home_location'] = df['home_location'].str.strip()
            
            # Format email to lowercase
            df['email'] = df['email'].str.lower()
            
            logger.info("Customer data cleaning completed")
        except Exception as e:
            logger.error(f"Error cleaning customer data: {str(e)}")
            raise
    
    def clean_product_data(self) -> None:
        """Clean dan standardize data product"""
        try:
            df = self.data['product']
            
            # Fill missing values
            df['baseColour'].fillna('Unknown', inplace=True)
            df['season'].fillna('All Season', inplace=True)
            df['usage'].fillna('Regular', inplace=True)
            
            # Standardize categories
            df['masterCategory'] = df['masterCategory'].str.title()
            df['subCategory'] = df['subCategory'].str.title()
            df['articleType'] = df['articleType'].str.title()
            
            logger.info("Product data cleaning completed")
        except Exception as e:
            logger.error(f"Error cleaning product data: {str(e)}")
            raise
    
    def create_customer_dimension(self) -> pd.DataFrame:
        """
        Create customer dimension table
        
        Returns:
            DataFrame untuk dimension table customer
        """
        try:
            customer_dim = self.data['customer'][[
                'customer_id', 'first_name', 'last_name', 'email', 
                'gender', 'home_location', 'home_country', 'first_join_date'
            ]].copy()
            
            # Add derived columns
            customer_dim['customer_since_days'] = (
                datetime.now() - customer_dim['first_join_date']
            ).dt.days
            
            logger.info("Customer dimension table created")
            return customer_dim
        except Exception as e:
            logger.error(f"Error creating customer dimension: {str(e)}")
            raise
    
    def create_product_dimension(self) -> pd.DataFrame:
        try:
            # Select kolom yang diperlukan
            product_dim = self.data['product'][[
                'id',
                'masterCategory',
                'subCategory',
                'articleType',
                'baseColour',
                'season',
                'year',
                'usage'
            ]].copy()

            # Konversi id ke int32 (sesuai dengan PostgreSQL INTEGER)
            product_dim['id'] = pd.to_numeric(product_dim['id'], errors='coerce').astype('int32')
    
            # Rename kolom
            product_dim = product_dim.rename(columns={
                'id': 'product_id',
                'masterCategory': 'master_category',
                'subCategory': 'sub_category',
                'articleType': 'article_type',
                'baseColour': 'base_colour'
            })

            # Validasi range untuk PostgreSQL INTEGER
            if (product_dim['product_id'] < -2147483648).any() or (product_dim['product_id'] > 2147483647).any():
                raise ValueError("product_id values outside INTEGER range")

            logger.info("Product dimension table created")
            return product_dim
        except Exception as e:
            logger.error(f"Error creating product dimension: {str(e)}")
            raise

    def create_sales_fact(self) -> pd.DataFrame:
        """
        Create sales fact table
        """
        try:
            sales_fact = self.data['transactions'].copy()
        
            # Kita tidak perlu lagi menggunakan loop 51 karena format metadata sudah berubah
            expected_columns = [
                'product_metadata_product_id',
                'product_metadata_quantity',
                'product_metadata_item_price'
            ]
        
            # Pastikan kolom yang dibutuhkan ada
            for col in expected_columns:
                if col not in sales_fact.columns:
                    logger.warning(f"Column {col} not found in sales_fact")
                    sales_fact[col] = None
        
            # Add time dimensions
            sales_fact['sale_date'] = sales_fact['created_at'].dt.date
            sales_fact['sale_month'] = sales_fact['created_at'].dt.to_period('M')
            sales_fact['sale_quarter'] = sales_fact['created_at'].dt.to_period('Q')
            sales_fact['sale_year'] = sales_fact['created_at'].dt.year
        
            logger.info("Sales fact table created")
            return sales_fact
        except Exception as e:
            logger.error(f"Error creating sales fact: {str(e)}")
            raise

    def transform_all(self) -> Dict[str, pd.DataFrame]:
        """
        Transform semua data dan create dimension & fact tables
        
        Returns:
            Dictionary berisi semua transformed DataFrames
        """
        try:
            # Clean dan transform data
            self.transform_timestamps()
            self.clean_customer_data()
            self.clean_product_data()
            
            # Create dimension dan fact tables
            transformed_data = {
                'dim_customer': self.create_customer_dimension(),
                'dim_product': self.create_product_dimension(),
                'fact_sales': self.create_sales_fact()
            }
            
            logger.info("All transformations completed")
            return transformed_data
        except Exception as e:
            logger.error(f"Error in transform_all: {str(e)}")
            raise
# src/extract/extractors.py

import pandas as pd
import json
from datetime import datetime
import logging
from typing import Dict, List, Any
import os
import csv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataExtractor:
    """Class untuk menghandle proses ekstraksi data dari file CSV"""
    
    def __init__(self, data_path: str):
        """
        Inisialisasi DataExtractor
        
        Args:
            data_path: Path ke directory data
        """
        self.data_path = data_path
        self.files = {
            'click_stream': 'click_stream.csv',
            'customer': 'customer.csv',
            'product': 'product.csv',
            'transactions': 'transactions.csv'
        }
    
    def read_csv_in_chunks(self, file_name: str, chunk_size: int = 10000) -> pd.DataFrame:
        """
        Membaca file CSV dalam chunks untuk menangani file besar
        
        Args:
            file_name: Nama file yang akan dibaca
            chunk_size: Ukuran chunk untuk pembacaan
            
        Returns:
            DataFrame yang sudah digabungkan
        """
        try:
            chunks = []
            for chunk in pd.read_csv(
                os.path.join(self.data_path, file_name),
                chunksize=chunk_size,
                on_bad_lines='warn'  # Tambahkan warning untuk bad lines
            ):
                chunks.append(chunk)
            return pd.concat(chunks, ignore_index=True)
        except Exception as e:
            logger.error(f"Error reading {file_name}: {str(e)}")
            raise
    
    def read_product_csv(self) -> pd.DataFrame:
        """
        Membaca product.csv dengan penanganan khusus
    
        Returns:
            DataFrame dari product.csv
        """
        try:
            # Coba baca file yang sudah dibersihkan terlebih dahulu
            cleaned_file_path = os.path.join(self.data_path, '..', 'processed', 'product_cleaned.csv')
        
            if os.path.exists(cleaned_file_path):
                logger.info("Menggunakan file product_cleaned.csv")
                file_path = cleaned_file_path
            else:
                logger.info("File cleaned tidak ditemukan, menggunakan product.csv original")
                file_path = os.path.join(self.data_path, self.files['product'])
        
            # Cek jumlah kolom yang benar dengan membaca header
            with open(file_path, 'r', encoding='utf-8') as f:
                header = next(csv.reader(f))
                num_columns = len(header)
        
            logger.info(f"Product CSV memiliki {num_columns} kolom")
        
            # Baca CSV dengan parameter tambahan untuk handling error
            df = pd.read_csv(
                file_path,
                escapechar='\\',        # Handle escape characters
                encoding='utf-8',       # Specific encoding
                on_bad_lines='skip',    # Skip bad lines
                low_memory=False,       # Disable low memory warnings
                quoting=csv.QUOTE_ALL   # Handle quoted values
            )
        
            logger.info(f"Berhasil membaca {len(df)} baris dari {os.path.basename(file_path)}")
        
            # Log informasi tentang kolom
            logger.info(f"Kolom yang terbaca: {df.columns.tolist()}")
        
            return df
        
        except Exception as e:
            logger.error(f"Error membaca product.csv: {str(e)}")
            raise
    
    def parse_json_column(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Parse kolom JSON menjadi kolom terpisah dengan pendekatan chunking"""
        try:
            chunk_size = 100000  # Proses per 100k baris
            result_chunks = []
        
            for start_idx in range(0, len(df), chunk_size):
                end_idx = min(start_idx + chunk_size, len(df))
                chunk_df = df.iloc[start_idx:end_idx].copy()
            
                # Handle NaN values
                chunk_df[column] = chunk_df[column].fillna('{}')
            
                # Parse JSON strings
                def safe_json_loads(x):
                    try:
                        if isinstance(x, str):
                            clean_str = x.replace("'", '"').strip()
                            return json.loads(clean_str)
                        return {}
                    except:
                        return {}
            
                parsed = chunk_df[column].apply(safe_json_loads)
            
                # Tentukan kolom berdasarkan tipe metadata
                if column == 'event_metadata':
                    # Kolom untuk event metadata
                    metadata_columns = {
                        'product_id': lambda x: x.get('product_id'),
                        'quantity': lambda x: x.get('quantity'),
                        'item_price': lambda x: x.get('item_price'),
                        'payment_status': lambda x: x.get('payment_status'),
                        'search_keywords': lambda x: x.get('search_keywords')
                    }
                else:  # product_metadata
                    # Parse array of products
                    def extract_first_product(x):
                        if isinstance(x, list) and len(x) > 0:
                            return x[0]
                        return {}
                
                    parsed = parsed.apply(extract_first_product)
                    metadata_columns = {
                        'product_id': lambda x: x.get('product_id'),
                        'quantity': lambda x: x.get('quantity'),
                        'item_price': lambda x: x.get('item_price')
                    }
            
                # Create expanded DataFrame
                expanded_df = pd.DataFrame(index=chunk_df.index)
                for col_name, extractor in metadata_columns.items():
                    expanded_df[f'{column}_{col_name}'] = parsed.apply(extractor)
            
                # Combine dengan kolom original
                result_chunk = pd.concat([chunk_df.drop(columns=[column]), expanded_df], axis=1)
                result_chunks.append(result_chunk)
            
                # Log progress
                logger.info(f"Processed rows {start_idx:,} to {end_idx:,} of {len(df):,}")
        
            # Combine semua chunks
            final_df = pd.concat(result_chunks, ignore_index=True)
            return final_df
        
        except Exception as e:
            logger.error(f"Error parsing JSON column {column}: {str(e)}")
            raise

    def extract_click_stream(self) -> pd.DataFrame:
        """Extract dan transform data click stream"""
        logger.info("Extracting click stream data...")
        df = self.read_csv_in_chunks(self.files['click_stream'])
        return self.parse_json_column(df, 'event_metadata')
    
    def extract_transactions(self) -> pd.DataFrame:
        """Extract dan transform data transactions"""
        logger.info("Extracting transaction data...")
        df = self.read_csv_in_chunks(self.files['transactions'])
        return self.parse_json_column(df, 'product_metadata')
    
    def extract_customer(self) -> pd.DataFrame:
        """Extract data customer"""
        logger.info("Extracting customer data...")
        return pd.read_csv(os.path.join(self.data_path, self.files['customer']))
    
    def extract_product(self) -> pd.DataFrame:
        """Extract data product"""
        logger.info("Extracting product data...")
        return self.read_product_csv()

    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extract semua data
        
        Returns:
            Dictionary berisi semua DataFrame
        """
        try:
            data = {
                'click_stream': self.extract_click_stream(),
                'transactions': self.extract_transactions(),
                'customer': self.extract_customer(),
                'product': self.extract_product()
            }
            logger.info("All data extracted successfully")
            return data
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise

    def validate_data(self, df: pd.DataFrame, expected_columns: List[str]) -> bool:
        """
        Validasi struktur data
        
        Args:
            df: DataFrame untuk divalidasi
            expected_columns: List kolom yang diharapkan
            
        Returns:
            True jika valid, False jika tidak
        """
        current_columns = set(df.columns)
        expected_columns = set(expected_columns)
        
        if not current_columns == expected_columns:
            logger.warning(f"Kolom tidak sesuai. Expected: {expected_columns}, Got: {current_columns}")
            return False
        return True
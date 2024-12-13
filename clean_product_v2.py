import pandas as pd
import numpy as np
import logging
import os
import csv  # Tambahkan import ini

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_product_data():
    try:
        input_file = 'data/raw/product.csv'
        output_file = 'data/processed/product_cleaned.csv'
        
        # Pastikan directory processed ada
        os.makedirs('data/processed', exist_ok=True)
        
        # Baca file dengan pandas
        logger.info("Membaca file product.csv...")
        df = pd.read_csv(input_file, on_bad_lines='skip')
        
        logger.info(f"Data awal: {len(df)} baris")
        
        # Bersihkan kolom id
        logger.info("Membersihkan kolom id...")
        
        # Konversi id ke integer dan tangani nilai yang invalid
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        
        # Hapus baris dengan id null atau di luar range INTEGER PostgreSQL
        max_int = 2147483647  # Maximum value for PostgreSQL INTEGER
        min_int = -2147483648  # Minimum value for PostgreSQL INTEGER
        
        invalid_ids = df[
            (df['id'].isnull()) | 
            (df['id'] > max_int) | 
            (df['id'] < min_int)
        ]
        
        if not invalid_ids.empty:
            logger.info(f"Menghapus {len(invalid_ids)} baris dengan id invalid")
            df = df[
                (df['id'].notnull()) & 
                (df['id'] <= max_int) & 
                (df['id'] >= min_int)
            ]
        
        # Pastikan id adalah integer
        df['id'] = df['id'].astype('int32')
        
        # Handle missing values di kolom lain
        df['baseColour'] = df['baseColour'].fillna('Unknown')
        df['season'] = df['season'].fillna('All Season')
        df['year'] = df['year'].fillna(0)
        df['usage'] = df['usage'].fillna('Regular')
        
        # Simpan file yang sudah dibersihkan
        logger.info(f"Menyimpan {len(df)} baris ke file bersih...")
        df.to_csv(output_file, index=False, quoting=csv.QUOTE_ALL)
        
        logger.info(f"File bersih disimpan di: {output_file}")
        logger.info("\nStatistik data bersih:")
        logger.info(f"Range id: {df['id'].min()} - {df['id'].max()}")
        logger.info(f"Jumlah baris: {len(df)}")
        logger.info(f"Kolom: {df.columns.tolist()}")
        
    except Exception as e:
        logger.error(f"Error dalam pembersihan data: {str(e)}")
        raise

if __name__ == "__main__":
    clean_product_data()
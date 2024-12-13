import pandas as pd
import json
from datetime import datetime
import os
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatasetAnalyzer:
    def __init__(self):
        # Dapatkan path ke direktori data/raw/
        self.data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'raw')
        
        self.files = {
            'click_stream': os.path.join(self.data_dir, 'click_stream.csv'),
            'customer': os.path.join(self.data_dir, 'customer.csv'),
            'product': os.path.join(self.data_dir, 'product.csv'),
            'transactions': os.path.join(self.data_dir, 'transactions.csv')
        }
        
        # Pastikan direktori data/raw/ ada
        if not os.path.exists(self.data_dir):
            logger.error(f"Directory tidak ditemukan: {self.data_dir}")
            logger.info("Current working directory: " + os.getcwd())
            raise FileNotFoundError(f"Directory tidak ditemukan: {self.data_dir}")
        
        # Log lokasi file
        logger.info("Menggunakan file-file berikut:")
        for name, path in self.files.items():
            logger.info(f"{name}: {path}")
            if not os.path.exists(path):
                logger.warning(f"File tidak ditemukan: {path}")
    
    def analyze_basic_stats(self, filename, df):
        """Menganalisis statistik umum file"""
        file_size = os.path.getsize(filename) / (1024 * 1024)  # Convert to MB
        rows = len(df)
        cols = len(df.columns)
        memory = df.memory_usage(deep=True).sum() / (1024 * 1024)  # Convert to MB
        
        print(f"\nStatistik untuk {filename}:")
        print(f"Ukuran File: {file_size:.2f} MB")
        print(f"Jumlah Baris: {rows:,}")
        print(f"Jumlah Kolom: {cols}")
        print(f"Memory Usage: {memory:.2f} MB")
        
        print("\nInformasi Kolom:")
        for col in df.columns:
            print(f"- {col}:")
            print(f"  Tipe Data: {df[col].dtype}")
            print(f"  Nilai Unik: {df[col].nunique():,}")
            print(f"  Nilai Null: {df[col].isnull().sum():,}")
    
    def analyze_all_files(self):
        """Menganalisis statistik dasar untuk semua file kecuali product.csv"""
        total_rows = 0
        total_size = 0
        
        for name, filename in self.files.items():
            if name != 'product':  # Skip product.csv for now
                try:
                    df = pd.read_csv(filename)
                    self.analyze_basic_stats(filename, df)
                    total_rows += len(df)
                    total_size += os.path.getsize(filename) / (1024 * 1024)
                except Exception as e:
                    logger.error(f"Error analyzing {filename}: {str(e)}")
        
        return total_rows, total_size
    
    def analyze_product_file(self):
        """Menganalisis product.csv secara terpisah"""
        try:
            df = pd.read_csv(self.files['product'], on_bad_lines='skip')
            self.analyze_basic_stats(self.files['product'], df)
            
            print("\nDetail Analisis Products:")
            print("=" * 50)
            
            print("\nDistribusi Master Categories:")
            print(df['masterCategory'].value_counts())
            
            print("\nDistribusi Sub Categories (top 10):")
            print(df['subCategory'].value_counts().head(10))
            
            print("\nDistribusi Article Types (top 10):")
            print(df['articleType'].value_counts().head(10))
            
            print("\nDistribusi Season:")
            print(df['season'].value_counts())
            
            print("\nRange Tahun Produk:")
            print(df['year'].value_counts().sort_index())
            
            return len(df), os.path.getsize(self.files['product']) / (1024 * 1024)
            
        except Exception as e:
            logger.error(f"Error analyzing product.csv: {str(e)}")
            return 0, 0
    
    def analyze_click_stream_details(self):
        """Analisis detail untuk click_stream"""
        try:
            df = pd.read_csv(self.files['click_stream'], nrows=100000)
            
            print("\nDetail Analisis Click Stream:")
            print("=" * 50)
            print("\nUnique Event Names:")
            print(df['event_name'].unique())
            print("\nEvent Name Counts:")
            print(df['event_name'].value_counts())
            
            print("\nSample Event Metadata (5 non-null values):")
            print(df[df['event_metadata'].notna()]['event_metadata'].head())
            
            df['event_time'] = pd.to_datetime(df['event_time'])
            print("\nRange Waktu Event:")
            print(f"Earliest: {df['event_time'].min()}")
            print(f"Latest: {df['event_time'].max()}")
            
        except Exception as e:
            logger.error(f"Error analyzing click stream details: {str(e)}")
    
    def analyze_transaction_details(self):
        """Analisis detail untuk transactions"""
        try:
            df = pd.read_csv(self.files['transactions'], nrows=100000)
            
            print("\nDetail Analisis Transactions:")
            print("=" * 50)
            print("\nUnique Payment Methods:")
            print(df['payment_method'].unique())
            print("\nPayment Method Counts:")
            print(df['payment_method'].value_counts())
            
            print("\nSample Product Metadata (5 records):")
            print(df['product_metadata'].head())
            
            df['created_at'] = pd.to_datetime(df['created_at'])
            print("\nRange Waktu Transaksi:")
            print(f"Earliest: {df['created_at'].min()}")
            print(f"Latest: {df['created_at'].max()}")
            
            print("\nUnique Promo Codes:")
            print(df['promo_code'].unique())
            
        except Exception as e:
            logger.error(f"Error analyzing transaction details: {str(e)}")
    
    def run_analysis(self):
        """Menjalankan semua analisis"""
        print("Starting Complete Analysis...")
        print("=" * 50)
        
        # Analyze all files except product.csv
        rows, size = self.analyze_all_files()
        
        # Analyze product.csv separately
        prod_rows, prod_size = self.analyze_product_file()
        
        # Run detailed analysis
        self.analyze_click_stream_details()
        self.analyze_transaction_details()
        
        # Print total summary
        print("\nRingkasan Total:")
        print("=" * 50)
        print(f"Total Jumlah Baris: {rows + prod_rows:,}")
        print(f"Total Ukuran File: {size + prod_size:.2f} MB")

def main():
    analyzer = DatasetAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
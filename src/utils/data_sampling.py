import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataSampler:
    def __init__(self, input_dir: str, output_dir: str, sample_size: float = 0.1):
        """
        Initialize DataSampler
        
        Parameters:
        -----------
        input_dir : str
            Directory containing raw CSV files
        output_dir : str
            Directory to save sampled CSV files
        sample_size : float
            Percentage of data to sample (default: 0.1 = 10%)
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.sample_size = sample_size
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def sample_customers(self) -> pd.DataFrame:
        """Sample customer data and maintain referential integrity"""
        logger.info("Sampling customer data...")
        
        # Read and sample customer data
        df_customers = pd.read_csv(self.input_dir / 'customer.csv')
        sampled_customers = df_customers.sample(
            n=int(len(df_customers) * self.sample_size),
            random_state=42
        )
        
        # Save sampled customers
        sampled_customers.to_csv(
            self.output_dir / 'customer_sampled.csv',
            index=False
        )
        logger.info(f"Sampled {len(sampled_customers)} customers")
        
        return sampled_customers['customer_id'].tolist()
    
    def sample_transactions(self, customer_ids: list) -> pd.DataFrame:
        """Sample transactions for selected customers"""
        logger.info("Sampling transaction data...")
        
        # Read transactions in chunks to handle large file
        chunk_size = 100000
        sampled_transactions = []
        
        for chunk in pd.read_csv(self.input_dir / 'transactions.csv', chunksize=chunk_size):
            # Filter transactions for sampled customers
            chunk_filtered = chunk[chunk['customer_id'].isin(customer_ids)]
            sampled_transactions.append(chunk_filtered)
        
        # Combine all chunks
        df_transactions = pd.concat(sampled_transactions, ignore_index=True)
        
        # Save sampled transactions
        df_transactions.to_csv(
            self.output_dir / 'transactions_sampled.csv',
            index=False
        )
        logger.info(f"Sampled {len(df_transactions)} transactions")
        
        return df_transactions['session_id'].tolist()
    
    def sample_clicks(self, session_ids: list):
        """Sample click stream data for selected sessions"""
        logger.info("Sampling click stream data...")
        
        # Process click stream in chunks due to size
        chunk_size = 100000
        sampled_clicks = []
        
        for chunk in pd.read_csv(self.input_dir / 'click_stream.csv', chunksize=chunk_size):
            # Filter clicks for sampled sessions
            chunk_filtered = chunk[chunk['session_id'].isin(session_ids)]
            sampled_clicks.append(chunk_filtered)
            
            # Log progress
            if len(sampled_clicks) % 10 == 0:
                logger.info(f"Processed {len(sampled_clicks)} chunks...")
        
        # Combine all chunks
        df_clicks = pd.concat(sampled_clicks, ignore_index=True)
        
        # Save sampled clicks
        df_clicks.to_csv(
            self.output_dir / 'click_stream_sampled.csv',
            index=False
        )
        logger.info(f"Sampled {len(df_clicks)} clicks")
    
    def sample_products(self):
        """Copy all product data as it's relatively small"""
        logger.info("Copying product data...")
        
        try:
            # Mencoba membaca dengan pandas default
            df_products = pd.read_csv(
                self.input_dir / 'product.csv',
                on_bad_lines='warn',  # Ubah dari error ke warn
                escapechar='\\',      # Tambahkan escape character
                encoding='utf-8'      # Specific encoding
            )
            logger.info("Successfully read products with default settings")
        except Exception as e:
            logger.warning(f"Failed to read with default settings: {str(e)}")
            logger.info("Trying alternative reading method...")
            
            try:
                # Membaca dengan metode alternatif
                df_products = pd.read_csv(
                    self.input_dir / 'product.csv',
                    on_bad_lines='skip',    # Skip bad lines
                    quoting=3,              # Quote none
                    encoding='utf-8',
                    dtype=str               # Baca semua sebagai string dulu
                )
                logger.info("Successfully read products with alternative settings")
            except Exception as e:
                logger.error(f"Failed to read products: {str(e)}")
                raise
        
        # Save all product data
        df_products.to_csv(
            self.output_dir / 'product_sampled.csv',
            index=False
        )
        logger.info(f"Copied {len(df_products)} products")
    
    def create_samples(self):
        """Create all sample datasets"""
        try:
            # Sample in order to maintain referential integrity
            customer_ids = self.sample_customers()
            session_ids = self.sample_transactions(customer_ids)
            self.sample_clicks(session_ids)
            self.sample_products()
            
            logger.info("Successfully created all sample datasets!")
            
        except Exception as e:
            logger.error(f"Error creating samples: {str(e)}")
            raise

def main():
    # Define directories
    input_dir = "data/raw"
    output_dir = "data/processed/sampled"
    
    # Create and run sampler
    sampler = DataSampler(
        input_dir=input_dir,
        output_dir=output_dir,
        sample_size=0.1  # 10% sample
    )
    sampler.create_samples()

if __name__ == "__main__":
    main()
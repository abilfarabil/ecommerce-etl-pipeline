import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Baca file dengan parameter tambahan untuk handling error
    df = pd.read_csv(
        'data/raw/product.csv',
        escapechar='\\',        # Handle escape characters
        encoding='utf-8',       # Specific encoding
        on_bad_lines='skip',    # Skip bad lines
        low_memory=False        # Disable low memory warnings
    )
    
    # Tampilkan 5 baris pertama
    print("\nSample 5 baris pertama:")
    print(df.head())
    
    # Tampilkan info dataframe
    print("\nInformasi detail dataframe:")
    print(df.info())
    
    # Tampilkan statistik untuk kolom id
    print("\nStatistik untuk kolom id:")
    print(df['id'].describe())
    
    # Tampilkan sample nilai id
    print("\nSample 10 nilai id:")
    print(df['id'].head(10).to_string())
    
    # Tampilkan nilai unik untuk master category
    print("\nNilai unik untuk masterCategory:")
    print(df['masterCategory'].unique())

except Exception as e:
    logger.error(f"Error reading file: {str(e)}")
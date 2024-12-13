import os
import sys
from pathlib import Path

# Get the project root directory
project_root = Path(__file__).parent.parent

# Add the project root to Python path
sys.path.append(str(project_root))

from src.extract.extractors import DataExtractor
from src.transform.transformers import DataTransformer

def test_etl():
    try:
        # Initialize extractor
        data_path = os.path.join(project_root, 'data', 'raw')
        extractor = DataExtractor(data_path)
        
        print("Starting data extraction...")
        # Extract data
        raw_data = extractor.extract_all()
        print("Data extraction completed")
        
        print("\nStarting data transformation...")
        # Initialize transformer
        transformer = DataTransformer(raw_data)
        
        # Transform data
        transformed_data = transformer.transform_all()
        print("Data transformation completed")
        
        # Print info
        for name, df in transformed_data.items():
            print(f"\nInfo for {name}:")
            print(df.info())
            print("\nFirst few rows:")
            print(df.head())
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    test_etl()
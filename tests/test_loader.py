import os
import sys
from pathlib import Path
import argparse

# Get the project root directory
project_root = Path(__file__).parent.parent

# Add the project root to Python path
sys.path.append(str(project_root))

from src.extract.extractors import DataExtractor
from src.transform.transformers import DataTransformer
from src.load.loaders import PostgresLoader

def test_full_etl(is_docker: bool = False):
    try:
        print("Starting ETL test...")
        
        # Extract
        print("\n1. Extracting data...")
        data_path = os.path.join(project_root, 'data', 'raw')
        extractor = DataExtractor(data_path)
        raw_data = extractor.extract_all()
        print("Extraction completed")
        
        # Transform
        print("\n2. Transforming data...")
        transformer = DataTransformer(raw_data)
        transformed_data = transformer.transform_all()
        print("Transformation completed")
        
        # Load
        print("\n3. Loading data to PostgreSQL...")
        loader = PostgresLoader(is_docker=is_docker)
        loader.load_all(transformed_data)
        print("Loading completed")
        
        print("\nETL process completed successfully!")
        
    except Exception as e:
        print(f"Error in ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--docker', action='store_true', help='Run in Docker mode')
    args = parser.parse_args()
    
    test_full_etl(is_docker=args.docker)
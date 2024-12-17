import psycopg2
from psycopg2.extras import execute_values
import logging
import os
import pandas as pd
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_database_connection():
    """Test koneksi ke database dan validasi schemas"""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get database credentials from environment variables
        db_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'ecommerce_dw'),
            'user': os.getenv('POSTGRES_USER', 'admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'admin123')
        }
        
        # Connect to database
        logger.info("Attempting to connect to database...")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Test query to check schemas
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('raw', 'staging', 'warehouse');
        """)
        schemas = cursor.fetchall()
        
        logger.info(f"Found schemas: {[schema[0] for schema in schemas]}")
        
        # Test loading sample data
        logger.info("Testing data loading...")
        df = pd.read_csv('data/processed/sampled/customer_sampled.csv', nrows=5)
        
        # Convert to list of tuples
        values = [tuple(x) for x in df.values]
        
        # Insert test data
        insert_query = """
            INSERT INTO raw.customers (
                customer_id, first_name, last_name, username, 
                email, gender, birthdate, device_type, 
                device_id, device_version, home_location_lat, 
                home_location_long, home_location, home_country, 
                first_join_date
            ) VALUES %s
            ON CONFLICT (customer_id) DO NOTHING;
        """
        
        execute_values(cursor, insert_query, values)
        conn.commit()
        
        # Verify data
        cursor.execute("SELECT COUNT(*) FROM raw.customers;")
        count = cursor.fetchone()[0]
        logger.info(f"Successfully loaded {count} test records")
        
        cursor.close()
        conn.close()
        logger.info("Database connection test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error testing database connection: {str(e)}")
        return False

if __name__ == "__main__":
    test_database_connection()
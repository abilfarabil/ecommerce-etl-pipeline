import psycopg2
import logging
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_database():
    """Initialize database with required schemas and tables"""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get database credentials
        db_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'ecommerce_dw'),
            'user': os.getenv('POSTGRES_USER', 'admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'admin123')
        }
        
        # Connect to database
        logger.info("Connecting to database...")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Create schemas
        logger.info("Creating schemas...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")
        
        # Create tables
        logger.info("Creating tables...")
        
        # Customers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.customers (
                customer_id INTEGER PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                username VARCHAR(100),
                email VARCHAR(255),
                gender VARCHAR(1),
                birthdate VARCHAR(100),
                device_type VARCHAR(50),
                device_id VARCHAR(100),
                device_version VARCHAR(50),
                home_location_lat DOUBLE PRECISION,
                home_location_long DOUBLE PRECISION,
                home_location VARCHAR(100),
                home_country VARCHAR(100),
                first_join_date TIMESTAMP
            );
        """)
        
        # Products table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.products (
                id INTEGER PRIMARY KEY,
                gender VARCHAR(50),
                master_category VARCHAR(100),
                sub_category VARCHAR(100),
                article_type VARCHAR(100),
                base_colour VARCHAR(50),
                season VARCHAR(50),
                year INTEGER,
                usage VARCHAR(50),
                product_display_name VARCHAR(255)
            );
        """)
        
        # Transactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.transactions (
                created_at TIMESTAMP,
                customer_id INTEGER,
                booking_id VARCHAR(100) PRIMARY KEY,
                session_id VARCHAR(100),
                product_metadata JSONB,
                payment_method VARCHAR(50),
                payment_status VARCHAR(50),
                promo_amount INTEGER,
                promo_code VARCHAR(50),
                shipment_fee INTEGER,
                shipment_date_limit TIMESTAMP,
                shipment_location_lat DOUBLE PRECISION,
                shipment_location_long DOUBLE PRECISION,
                total_amount INTEGER
            );
        """)
        
        # Click stream table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.click_stream (
                session_id VARCHAR(100),
                event_name VARCHAR(50),
                event_time TIMESTAMP,
                event_id VARCHAR(100) PRIMARY KEY,
                traffic_source VARCHAR(50),
                event_metadata JSONB
            );
        """)
        
        # Create indexes
        logger.info("Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_customer_id ON raw.transactions(customer_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_session_id ON raw.click_stream(session_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_time ON raw.click_stream(event_time);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transaction_time ON raw.transactions(created_at);")
        
        # Grant permissions
        logger.info("Granting permissions...")
        cursor.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO admin;")
        cursor.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO admin;")
        cursor.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO admin;")
        
        # Commit changes
        conn.commit()
        
        logger.info("Database initialization completed successfully!")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

if __name__ == "__main__":
    init_database()
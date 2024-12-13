import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def reset_database():
    try:
        # Connect ke default database
        conn = psycopg2.connect(
            dbname='admin',
            user='admin',
            password='admin123',
            host='localhost',
            port='5432'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Terminate existing connections
        logger.info("Terminating existing connections...")
        try:
            cursor.execute("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = 'ecommerce_dw'
                AND pid <> pg_backend_pid();
            """)
            # Tunggu sebentar untuk memastikan semua koneksi sudah terputus
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Error terminating connections: {str(e)}")
        
        # Drop database jika ada
        logger.info("Dropping database if exists...")
        cursor.execute("DROP DATABASE IF EXISTS ecommerce_dw;")
        
        # Create database baru
        logger.info("Creating new database...")
        cursor.execute("CREATE DATABASE ecommerce_dw;")
        
        cursor.close()
        conn.close()
        
        logger.info("Database reset completed successfully!")
        
    except Exception as e:
        logger.error(f"Error resetting database: {str(e)}")
        raise

if __name__ == "__main__":
    # Stop docker containers terlebih dahulu
    logger.info("Please make sure to stop all Docker containers first!")
    input("Press Enter to continue...")
    reset_database()
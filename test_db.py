import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connections():
    # Test koneksi ke database ecommerce_dw
    try:
        conn = psycopg2.connect(
            dbname='ecommerce_dw',
            user='admin',
            password='admin123',
            host='localhost',
            port='5432'
        )
        logger.info("Berhasil koneksi ke database ecommerce_dw")
        conn.close()
    except Exception as e:
        logger.error(f"Error koneksi ke ecommerce_dw: {str(e)}")

    # Test koneksi ke database admin
    try:
        conn = psycopg2.connect(
            dbname='admin',
            user='admin',
            password='admin123',
            host='localhost',
            port='5432'
        )
        logger.info("Berhasil koneksi ke database admin")
        conn.close()
    except Exception as e:
        logger.error(f"Error koneksi ke admin: {str(e)}")

if __name__ == "__main__":
    test_connections()
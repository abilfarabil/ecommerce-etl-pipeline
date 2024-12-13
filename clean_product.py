import csv
import os
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_product_csv():
    try:
        input_file = 'data/raw/product.csv'
        output_file = 'data/processed/product_cleaned.csv'
        
        # Pastikan directory processed ada
        os.makedirs('data/processed', exist_ok=True)
        
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            
            # Tulis header
            header = next(reader)
            writer.writerow(header)
            
            # Proses setiap baris
            rows_processed = 0
            rows_fixed = 0
            
            for row in reader:
                rows_processed += 1
                
                # Jika baris memiliki lebih dari 10 kolom
                if len(row) > 10:
                    # Gabungkan kolom berlebih ke kolom terakhir
                    row = row[:9] + [','.join(row[9:])]
                    rows_fixed += 1
                
                writer.writerow(row)
                
                # Log progress setiap 10000 baris
                if rows_processed % 10000 == 0:
                    logger.info(f"Processed {rows_processed} rows...")
            
            logger.info(f"Cleaning completed:")
            logger.info(f"Total rows processed: {rows_processed}")
            logger.info(f"Rows fixed: {rows_fixed}")
            logger.info(f"Cleaned file saved as: {output_file}")
            
    except Exception as e:
        logger.error(f"Error cleaning CSV: {str(e)}")
        raise

if __name__ == "__main__":
    clean_product_csv()
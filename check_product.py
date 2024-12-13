import pandas as pd
import numpy as np

def check_product_csv():
    try:
        # Baca file dengan parameter tambahan untuk debugging
        df = pd.read_csv(
            'data/raw/product.csv',
            on_bad_lines='warn',  # Akan memberikan warning untuk baris yang bermasalah
            error_bad_lines=False  # Akan skip baris yang bermasalah
        )
        
        print("\nInformasi Dataset:")
        print(f"Jumlah baris: {len(df)}")
        print(f"Jumlah kolom: {len(df.columns)}")
        print("\nNama kolom:")
        print(df.columns.tolist())
        
        print("\nDetail Product ID:")
        print(f"Tipe data: {df['id'].dtype}")
        print(f"Nilai minimum: {df['id'].min()}")
        print(f"Nilai maximum: {df['id'].max()}")
        print(f"\nSample values (first 10):\n{df['id'].head(10)}")
        
        # Cek distribusi nilai
        print("\nDistribusi nilai:")
        print(df['id'].describe())
        
        # Cek jika ada nilai yang tidak valid
        print("\nCek nilai tidak valid:")
        invalid_ids = df[~df['id'].apply(lambda x: isinstance(x, (int, np.int64)))]
        if len(invalid_ids) > 0:
            print(f"Ditemukan {len(invalid_ids)} nilai tidak valid:")
            print(invalid_ids['id'])
        else:
            print("Semua nilai valid")
        
        # Cek baris yang memiliki tanda koma (potential delimiter issues)
        print("\nCek potensi masalah delimiter:")
        for col in df.columns:
            if df[col].dtype == 'object':
                rows_with_comma = df[df[col].astype(str).str.contains(',', na=False)]
                if len(rows_with_comma) > 0:
                    print(f"\nKolom {col} memiliki {len(rows_with_comma)} baris dengan koma:")
                    print(rows_with_comma.head())

    except Exception as e:
        print(f"Error: {str(e)}")
        
        # Coba baca file secara manual untuk melihat baris yang bermasalah
        print("\nMencoba membaca file secara manual...")
        with open('data/raw/product.csv', 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if len(line.split(',')) != 10:  # Seharusnya 10 kolom
                    print(f"\nBaris {i} memiliki {len(line.split(','))} kolom:")
                    print(line.strip())
                if i == 6050:  # Baca sampai beberapa baris setelah baris yang error
                    break

if __name__ == "__main__":
    check_product_csv()
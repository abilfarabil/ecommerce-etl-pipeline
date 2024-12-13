import pandas as pd

# Baca transactions.csv
df = pd.read_csv('data/raw/transactions.csv')

# Lihat beberapa baris pertama kolom product_metadata
print("Sample product_metadata:")
print(df['product_metadata'].head())
print("\nTipe data:")
print(df['product_metadata'].dtype)
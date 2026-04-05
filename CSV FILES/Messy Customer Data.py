import pandas as pd
from sqlalchemy import create_engine

# Connection settings
YOUR_PASSWORD = "qweflx7123"  # Change to your PostgreSQL password
conn_string = f"postgresql://postgres:{YOUR_PASSWORD}@localhost:5432/superstore_elt"
engine = create_engine(conn_string)

# Load your CSV (change filename to match yours)
df = pd.read_csv("messy_customer_data.csv")  # or whatever your file is named

# See what you're loading
print(f"Loaded {len(df)} rows")
print(f"Columns: {df.columns.tolist()}")
print("\nFirst 3 rows:")
print(df.head(3))

# Load to PostgreSQL
df.to_sql("raw_customers", engine, if_exists="replace", index=False)
print("\n✅ Loaded to PostgreSQL table 'raw_customers'")
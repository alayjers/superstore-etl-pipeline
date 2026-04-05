import pandas as pd
from sqlalchemy import create_engine, text

YOUR_PASSWORD = "qweflx7123"
conn_string = f"postgresql://postgres:{YOUR_PASSWORD}@localhost:5432/superstore_elt"
engine = create_engine(conn_string)

tables = ['category_summary', 'region_summary', 'segment_summary', 'monthly_trend', 'top_products']

for table in tables:
    df = pd.read_sql(f'SELECT * FROM "{table}"', engine)
    df.to_csv(f"{table}.csv", index=False)
    print(f"Saved {table}.csv")
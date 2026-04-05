import pandas as pd
from sqlalchemy import create_engine, text

YOUR_PASSWORD = "qweflx7123"
conn_string = f"postgresql://postgres:{YOUR_PASSWORD}@localhost:5432/superstore_elt"
engine = create_engine(conn_string)

# Export main summary tables
tables = ['category_summary', 'region_summary', 'segment_summary', 'monthly_trend', 'top_products']
for table in tables:
    df = pd.read_sql(f'SELECT * FROM "{table}"', engine)
    df.to_csv(f"{table}.csv", index=False)
    print(f"Saved {table}.csv")

# Export KPI data
kpi_query = """
    SELECT 
        ROUND(SUM("Sales")::numeric, 0) as Total_Sales,
        COUNT(*) as Number_of_Orders,
        ROUND(AVG("Sales")::numeric, 0) as Avg_Order_Value
    FROM sales_cleaned
"""
df_kpi = pd.read_sql(kpi_query, engine)
df_kpi.to_csv("kpi_data.csv", index=False)
print("Saved kpi_data.csv")

print("\nAll CSVs exported successfully!")
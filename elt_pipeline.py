import pandas as pd
from sqlalchemy import create_engine, text

print("=" * 50)
print("ELT PIPELINE WITH POSTGRESQL")
print("=" * 50)


df = pd.read_csv("C:/Users/gutie/.cache/kagglehub/datasets/rohitsahoo/sales-forecasting/versions/2/train.csv")
print(f"Extracted {len(df)} rows")

print("\n[LOADING] Raw data to PostgreSQL...")

YOUR_PASSWORD = "password" #not my password

conn_string = f"postgresql://postgres:{YOUR_PASSWORD}@localhost:5432/superstore_elt"
engine = create_engine(conn_string)


df.to_sql("raw_sales", engine, if_exists="replace", index=False)
print(f"Loaded {len(df)} rows into 'raw_sales' table")


print("\n[TRANSFORMING] Using SQL...")

with engine.connect() as conn:

    conn.execute(text("""
        CREATE TABLE sales_cleaned AS
        SELECT 
            *,
            TO_DATE("Order Date", 'DD/MM/YYYY') as Order_Date,
            CASE 
                WHEN "Sales" > 500 THEN 'High'
                WHEN "Sales" > 100 THEN 'Medium'
                ELSE 'Low'
            END as Sales_Tier
        FROM raw_sales
        WHERE "Sales" IS NOT NULL
    """))
    conn.commit()
    print("  - Created sales_cleaned table")


    conn.execute(text("""
        CREATE TABLE category_summary AS
        SELECT 
            "Category",
            ROUND(SUM("Sales")::numeric, 2) as Total_Sales,
            COUNT(*) as Number_of_Orders,
            ROUND(AVG("Sales")::numeric, 2) as Avg_Order_Value
        FROM sales_cleaned
        GROUP BY "Category"
        ORDER BY Total_Sales DESC
    """))
    conn.commit()
    print("  - Created category_summary")


    conn.execute(text("""
        CREATE TABLE region_summary AS
        SELECT 
            "Region",
            ROUND(SUM("Sales")::numeric, 2) as Total_Sales,
            COUNT(*) as Number_of_Orders
        FROM sales_cleaned
        GROUP BY "Region"
        ORDER BY Total_Sales DESC
    """))
    conn.commit()
    print("  - Created region_summary")


    conn.execute(text("""
        CREATE TABLE segment_summary AS
        SELECT 
            "Segment",
            ROUND(SUM("Sales")::numeric, 2) as Total_Sales,
            COUNT(*) as Number_of_Orders
        FROM sales_cleaned
        GROUP BY "Segment"
        ORDER BY Total_Sales DESC
    """))
    conn.commit()
    print("  - Created segment_summary")


    conn.execute(text("""
        CREATE TABLE monthly_trend AS
        SELECT 
            EXTRACT(YEAR FROM Order_Date) as Year,
            EXTRACT(MONTH FROM Order_Date) as Month,
            TO_CHAR(Order_Date, 'Month') as Month_Name,
            ROUND(SUM("Sales")::numeric, 2) as Total_Sales
        FROM sales_cleaned
        GROUP BY Year, Month, Month_Name
        ORDER BY Year, Month
    """))
    conn.commit()
    print("  - Created monthly_trend")


    conn.execute(text("""
        CREATE TABLE top_products AS
        SELECT 
            "Product Name",
            ROUND(SUM("Sales")::numeric, 2) as Total_Sales
        FROM sales_cleaned
        GROUP BY "Product Name"
        ORDER BY Total_Sales DESC
        LIMIT 10
    """))
    conn.commit()
    print("  - Created top_products")


    conn.execute(text("""
        CREATE TABLE region_category AS
        SELECT 
            "Region",
            "Category",
            ROUND(SUM("Sales")::numeric, 2) as Total_Sales
        FROM sales_cleaned
        GROUP BY "Region", "Category"
        ORDER BY "Region", Total_Sales DESC
    """))
    conn.commit()
    print("  - Created region_category")


print("\n" + "=" * 50)
print("QUERY RESULTS")
print("=" * 50)

with engine.connect() as conn:
    # Category summary
    result = conn.execute(text("SELECT * FROM category_summary"))
    print("\nCATEGORY SUMMARY:")
    for row in result:
        print(f"  {row[0]}: ${row[1]:,.2f} ({row[2]} orders, avg ${row[3]:,.2f})")


    result = conn.execute(text("SELECT * FROM region_summary"))
    print("\nREGION SUMMARY:")
    for row in result:
        print(f"  {row[0]}: ${row[1]:,.2f} ({row[2]} orders)")


    result = conn.execute(text("SELECT * FROM segment_summary"))
    print("\nSEGMENT SUMMARY:")
    for row in result:
        print(f"  {row[0]}: ${row[1]:,.2f} ({row[2]} orders)")


    result = conn.execute(text("SELECT * FROM top_products"))
    print("\nTOP 5 PRODUCTS:")
    for i, row in enumerate(result.fetchmany(5), 1):
        product_name = row[0][:45]
        print(f"  {i}. {product_name}... ${row[1]:,.2f}")


print("\n[EXPORTING] Results to CSV...")

tables = ['category_summary', 'region_summary', 'segment_summary', 'monthly_trend', 'top_products', 'region_category']

for table in tables:
    df_export = pd.read_sql(f'SELECT * FROM "{table}"', engine)
    df_export.to_csv(f"postgres_{table}.csv", index=False)
    print(f"  - Exported postgres_{table}.csv")


print("\n" + "=" * 50)
print("VERIFICATION")
print("=" * 50)

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """))

    tables = result.fetchall()
    print("Tables in database:")
    for table in tables:
        if table[0] not in ['raw_sales', 'sales_cleaned']:
            count = conn.execute(text(f'SELECT COUNT(*) FROM "{table[0]}"')).fetchone()[0]
            print(f"  - {table[0]}: {count} rows")

print("\n" + "=" * 50)
print("ELT PIPELINE COMPLETE!")
print("=" * 50)
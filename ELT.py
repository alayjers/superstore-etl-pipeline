import pandas as pd
import matplotlib.pyplot as plt

#path = kagglehub.dataset_download("rohitsahoo/sales-forecasting")
#print(f"Dataset Downloaded to {path}")



df = pd.read_csv("C:/Users/gutie/.cache/kagglehub/datasets/rohitsahoo/sales-forecasting/versions/2/train.csv")
print(f"Loaded {len(df)} rows")


date_column = 'Order Date'
df[date_column] = pd.to_datetime(df[date_column], format='%d/%m/%Y')


df['Year'] = df[date_column].dt.year
df['Month'] = df[date_column].dt.month
df['Month_Name'] = df[date_column].dt.strftime('%B')
df['Day_of_Week'] = df[date_column].dt.day_name()


df = df.dropna(subset=['Sales'])


category_summary = df.groupby('Category')['Sales'].sum().round(2).reset_index()
region_summary = df.groupby('Region')['Sales'].sum().round(2).reset_index()
region_summary = region_summary.sort_values('Sales', ascending=False)
segment_summary = df.groupby('Segment')['Sales'].sum().round(2).reset_index()
monthly_trend = df.groupby(['Year', 'Month', 'Month_Name'])['Sales'].sum().round(2).reset_index()
monthly_trend = monthly_trend.sort_values(['Year', 'Month'])
top_products = df.groupby('Product Name')['Sales'].sum().sort_values(ascending=False).head(10)


print("\nSALES BY CATEGORY:")
print(category_summary)
print("\nSALES BY REGION:")
print(region_summary)
print("\nSALES BY SEGMENT:")
print(segment_summary)
print("\nTOP 10 PRODUCTS:")
print(top_products)


category_summary.to_csv("category_summary.csv", index=False)
region_summary.to_csv("region_summary.csv", index=False)
segment_summary.to_csv("segment_summary.csv", index=False)
monthly_trend.to_csv("monthly_trend.csv", index=False)
df.to_csv("cleaned_sales_data.csv", index=False)



print(f"TOTAL SALES: ${df['Sales'].sum():,.2f}")
print(f"AVERAGE SALE: ${df['Sales'].mean():,.2f}")
print(f"NUMBER OF ORDERS: {len(df):,}")



category_summary.plot(x='Category', y='Sales', kind='bar', legend=False, color='skyblue')
plt.title('Sales by Category', fontsize=14)
plt.ylabel('Total Sales ($)', fontsize=12)
plt.xlabel('Category', fontsize=12)
plt.tight_layout()
plt.savefig('sales_by_category.png')
print("\nChart saved as 'sales_by_category.png'")
plt.show()


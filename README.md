# Superstore ETL Pipeline

ETL pipeline that extracts Superstore sales data from CSV, transforms it using Python/pandas, and loads cleaned results to CSV files.

## Tech Stack
- Python 3.x
- pandas
- NumPy

## What This Pipeline Does

### Extract
- Loads 9,800 rows from Superstore CSV file

### Transform
- Converts dates to datetime format
- Extracts Year, Month, Month_Name, Day_of_Week
- Removes rows with missing values
- Calculates profit margin = (Profit / Sales) * 100
- Flags high-value orders (profit > $100)
- Aggregates sales by category, region, segment, and month

### Load
- Exports cleaned data to CSV files

## Output Files

| File | Content |
| :--- | :--- |
| `cleaned_superstore.csv` | Full cleaned dataset |
| `category_summary.csv` | Sales by product category |
| `region_summary.csv` | Sales by region |
| `segment_summary.csv` | Sales by customer segment |
| `monthly_trend.csv` | Monthly sales trends |
| `top_products.csv` | Top 10 selling products |

## How to Run

```bash
pip install pandas numpy
python etl_pipeline.py

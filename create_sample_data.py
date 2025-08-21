"""
Script to create sample data files for testing the ETL pipeline.
This will generate realistic sample Excel and CSV files with various data types.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

# Set random seed for reproducible results
np.random.seed(42)
random.seed(42)

def create_sales_data(num_records=1000):
    """Create sample sales data."""
    
    # Generate date range
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 31)
    date_range = pd.date_range(start_date, end_date, freq='D')
    
    # Product categories and names
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys']
    products = {
        'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Camera'],
        'Clothing': ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Shoes'],
        'Books': ['Fiction Novel', 'Textbook', 'Cookbook', 'Biography', 'Self-Help'],
        'Home & Garden': ['Furniture', 'Kitchenware', 'Bedding', 'Tools', 'Plants'],
        'Sports': ['Running Shoes', 'Gym Equipment', 'Sports Apparel', 'Outdoor Gear', 'Supplements'],
        'Toys': ['Action Figure', 'Board Game', 'Educational Toy', 'Puzzle', 'Doll']
    }
    
    # Regions and sales reps
    regions = ['North', 'South', 'East', 'West', 'Central']
    sales_reps = ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Eva Brown', 
                  'Frank Miller', 'Grace Lee', 'Henry Taylor', 'Ivy Chen', 'Jack Anderson']
    
    # Generate sample data
    data = []
    for _ in range(num_records):
        category = random.choice(categories)
        product = random.choice(products[category])
        
        # Generate realistic sales data
        base_price = random.uniform(10, 500)
        quantity = random.randint(1, 20)
        discount = random.uniform(0, 0.3)  # 0-30% discount
        
        unit_price = base_price * (1 - discount)
        total_amount = unit_price * quantity
        
        record = {
            'Date': random.choice(date_range.tolist()),
            'Product_Category': category,
            'Product_Name': product,
            'Quantity': quantity,
            'Unit_Price': round(unit_price, 2),
            'Total_Amount': round(total_amount, 2),
            'Discount_Percent': round(discount * 100, 1),
            'Sales_Rep': random.choice(sales_reps),
            'Region': random.choice(regions),
            'Customer_Type': random.choice(['Individual', 'Business', 'Government']),
            'Payment_Method': random.choice(['Credit Card', 'Cash', 'Bank Transfer', 'Check'])
        }
        data.append(record)
    
    return pd.DataFrame(data)

def create_customer_data(num_records=500):
    """Create sample customer data."""
    
    # Customer data
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Lisa', 'Robert', 'Emily', 
                   'William', 'Jessica', 'James', 'Ashley', 'Christopher', 'Amanda', 'Daniel']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
                  'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson']
    
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 
              'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville']
    
    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'FL']
    
    data = []
    for i in range(num_records):
        customer_id = f"CUST_{i+1:05d}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        record = {
            'Customer_ID': customer_id,
            'First_Name': first_name,
            'Last_Name': last_name,
            'Email': f"{first_name.lower()}.{last_name.lower()}@email.com",
            'Phone': f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}",
            'City': random.choice(cities),
            'State': random.choice(states),
            'ZIP_Code': f"{random.randint(10000, 99999)}",
            'Age': random.randint(18, 80),
            'Gender': random.choice(['Male', 'Female', 'Other']),
            'Registration_Date': datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460)),
            'Total_Purchases': random.randint(1, 50),
            'Total_Spent': round(random.uniform(50, 5000), 2),
            'Loyalty_Status': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'])
        }
        data.append(record)
    
    return pd.DataFrame(data)

def create_inventory_data(num_records=200):
    """Create sample inventory data."""
    
    # Product data
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys']
    suppliers = ['Supplier A', 'Supplier B', 'Supplier C', 'Supplier D', 'Supplier E']
    warehouses = ['Warehouse North', 'Warehouse South', 'Warehouse East', 'Warehouse West']
    
    data = []
    for i in range(num_records):
        product_id = f"PROD_{i+1:05d}"
        category = random.choice(categories)
        
        # Generate realistic inventory data
        cost_price = random.uniform(5, 200)
        selling_price = cost_price * random.uniform(1.2, 3.0)  # 20-200% markup
        
        record = {
            'Product_ID': product_id,
            'Product_Name': f"{category} Item {i+1}",
            'Category': category,
            'Supplier': random.choice(suppliers),
            'Warehouse': random.choice(warehouses),
            'Cost_Price': round(cost_price, 2),
            'Selling_Price': round(selling_price, 2),
            'Stock_Quantity': random.randint(0, 1000),
            'Reorder_Level': random.randint(10, 100),
            'Last_Restocked': datetime.now() - timedelta(days=random.randint(1, 90)),
            'Expiry_Date': datetime.now() + timedelta(days=random.randint(30, 730)) if random.random() > 0.7 else None,
            'Status': random.choice(['Active', 'Discontinued', 'Out of Stock'])
        }
        data.append(record)
    
    return pd.DataFrame(data)

def create_financial_data(num_records=300):
    """Create sample financial data."""
    
    # Account types and categories
    account_types = ['Revenue', 'Expense', 'Asset', 'Liability', 'Equity']
    expense_categories = ['Marketing', 'Operations', 'Salaries', 'Rent', 'Utilities', 'Travel', 'Equipment']
    revenue_categories = ['Product Sales', 'Service Revenue', 'Interest Income', 'Other Income']
    
    data = []
    for i in range(num_records):
        account_type = random.choice(account_types)
        
        if account_type == 'Expense':
            category = random.choice(expense_categories)
            amount = -random.uniform(100, 10000)  # Negative for expenses
        elif account_type == 'Revenue':
            category = random.choice(revenue_categories)
            amount = random.uniform(500, 50000)  # Positive for revenue
        else:
            category = account_type
            amount = random.uniform(-20000, 20000)
        
        record = {
            'Transaction_ID': f"TXN_{i+1:06d}",
            'Date': datetime(2023, 1, 1) + timedelta(days=random.randint(0, 395)),
            'Account_Type': account_type,
            'Category': category,
            'Description': f"{category} transaction {i+1}",
            'Amount': round(amount, 2),
            'Reference': f"REF_{random.randint(1000, 9999)}",
            'Department': random.choice(['Sales', 'Marketing', 'Operations', 'HR', 'Finance', 'IT']),
            'Approved_By': random.choice(['Manager A', 'Manager B', 'Manager C', 'CFO', 'CEO'])
        }
        data.append(record)
    
    return pd.DataFrame(data)

def main():
    """Generate all sample data files."""
    
    # Create output directory
    output_dir = Path("data/input")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("Generating sample data files...")
    
    # Generate sales data
    print("Creating sales data...")
    sales_df = create_sales_data(1000)
    
    # Save as both Excel and CSV
    sales_df.to_excel(output_dir / "sales_data_2023.xlsx", index=False)
    sales_df.to_csv(output_dir / "sales_data_2023.csv", index=False)
    
    # Create additional sales files with different date ranges
    sales_q1 = sales_df[sales_df['Date'] <= '2023-03-31']
    sales_q1.to_excel(output_dir / "sales_q1_2023.xlsx", index=False)
    
    sales_q2 = sales_df[(sales_df['Date'] > '2023-03-31') & (sales_df['Date'] <= '2023-06-30')]
    sales_q2.to_csv(output_dir / "sales_q2_2023.csv", index=False)
    
    # Generate customer data
    print("Creating customer data...")
    customer_df = create_customer_data(500)
    customer_df.to_excel(output_dir / "customer_database.xlsx", index=False)
    customer_df.to_csv(output_dir / "customer_export.csv", index=False)
    
    # Generate inventory data
    print("Creating inventory data...")
    inventory_df = create_inventory_data(200)
    inventory_df.to_excel(output_dir / "inventory_report.xlsx", index=False)
    inventory_df.to_csv(output_dir / "current_inventory.csv", index=False)
    
    # Generate financial data
    print("Creating financial data...")
    financial_df = create_financial_data(300)
    financial_df.to_excel(output_dir / "financial_transactions.xlsx", index=False)
    financial_df.to_csv(output_dir / "accounting_data.csv", index=False)
    
    # Create some files with missing data and duplicates for testing data cleaning
    print("Creating test files with data quality issues...")
    
    # Sales data with missing values
    sales_with_missing = sales_df.copy()
    # Randomly set some values to NaN
    for col in ['Unit_Price', 'Sales_Rep', 'Region']:
        mask = np.random.random(len(sales_with_missing)) < 0.1  # 10% missing
        sales_with_missing.loc[mask, col] = np.nan
    
    sales_with_missing.to_excel(output_dir / "sales_with_missing_data.xlsx", index=False)
    
    # Customer data with duplicates
    customer_with_dupes = customer_df.copy()
    # Add some duplicate rows
    duplicates = customer_df.sample(n=50)
    customer_with_dupes = pd.concat([customer_with_dupes, duplicates], ignore_index=True)
    customer_with_dupes.to_csv(output_dir / "customer_with_duplicates.csv", index=False)
    
    print(f"\nSample data files created in {output_dir}:")
    for file in output_dir.glob("*"):
        if file.is_file():
            print(f"  - {file.name}")
    
    print(f"\nTotal files created: {len(list(output_dir.glob('*')))}")
    print("Sample data generation completed!")

if __name__ == "__main__":
    main()


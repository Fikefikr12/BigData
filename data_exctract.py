import pandas as pd
import os

# Manually define the file paths for five CSV files
csv_files = [
    "csvData/df_Customers.csv",
    "csvData/df_OrderItems.csv",
    "csvData/df_Orders.csv",
    "csvData/df_Payments.csv",
    "csvData/df_Products.csv"
]

# Verify that all files exist before proceeding
for file in csv_files:
    if not os.path.exists(file):
        raise FileNotFoundError(f"File not found: {file}")

# Read and merge the CSV files
df_list = []  # List to store individual DataFrames
for file in csv_files:
    temp_df = pd.read_csv(file)  # Read each CSV file
    temp_df["source_file"] = os.path.basename(file)  # Add column to track source file
    df_list.append(temp_df)

# Concatenate all DataFrames into one
df = pd.concat(df_list, ignore_index=True)

# Display dataset details
print("Dataset Loaded Successfully from 5 CSV Files!")
print("Total Rows:", df.shape[0])
print("Total Columns:", df.shape[1])
print("\nColumn Names:\n", df.columns.tolist())
print("\nDataset Info:")
print(df.info())  # Check data types and missing values
print("\nFirst 5 Rows:")
print(df.head())  # Preview first few rows

import pandas as pd
from data_exctract import csv_files

# Ensure csv_files is a list of DataFrames
# If csv_files is a list of file paths (strings), read them into DataFrames
if isinstance(csv_files, list):
    # Check if the first element is a string (file path)
    if isinstance(csv_files[0], str):
        # Read all CSV files into DataFrames
        df_list = [pd.read_csv(file) for file in csv_files]
        df = pd.concat(df_list, ignore_index=True)
    else:
        # If it's already a list of DataFrames, concatenate them
        df = pd.concat(csv_files, ignore_index=True)
else:
    # If csv_files is a single DataFrame, use it directly
    df = csv_files

# -------------- Step 1: Handle Missing Values --------------
# Drop rows where all values are missing
df.dropna(how='all', inplace=True)

# Fill missing values for categorical columns with mode (most frequent value)
categorical_cols = df.select_dtypes(include=['object']).columns
df[categorical_cols] = df[categorical_cols].fillna(df[categorical_cols].mode().iloc[0])

# Fill missing values for numerical columns with median
numerical_cols = df.select_dtypes(include=['number']).columns
df[numerical_cols] = df[numerical_cols].fillna(df[numerical_cols].median())

# -------------- Step 2: Remove Duplicates --------------
df.drop_duplicates(inplace=True)

# -------------- Step 3: Format Data Types --------------
# Convert date columns to datetime format
date_columns = ['order_purchase_timestamp', 'order_approved_at']  # Updated based on your dataset

for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')  # Convert, handling errors

# Convert numerical columns to appropriate types
for col in numerical_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert, handling errors

# -------------- Step 4: Handle Inconsistencies & Errors --------------
# Standardizing categorical values (example: 'Yes', 'yes', 'YES' → 'Yes')
for col in categorical_cols:
    if df[col].dtype == 'object':  # Ensure the column is of string type
        df[col] = df[col].str.strip().str.lower().str.capitalize()

# Removing invalid price values (assuming 'price' column exists)
if 'price' in df.columns:
    df = df[df['price'] > 0]  # Remove negative or zero prices

# -------------- Display Final Cleaned Data --------------
print("Data Cleaning Completed!")
print("Total Rows After Cleaning:", df.shape[0])
print("Total Columns:", df.shape[1])
print("\nCleaned Data Info:")
df.to_csv("cleande_data.csv",index=False)
print(df.info())
print("\nFirst 5 Rows of Cleaned Data:")
print(df.head())
import os
import glob
import sqlite3
import pandas as pd
import tkinter as tk
from tkinter.filedialog import askopenfilename

def select_excel_file(name):
    root = tk.Tk()
    root.title(f"Select specific file : {name}")
    root.withdraw()
    print(f"Select the Excel file: {name}")
    file_path = askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
    if not file_path:
        print("No file selected.")
        exit()
    return file_path

def get_latest_excel_file(folder_path, keyword, name_for_manual="Excel file"):
    # Search for .xlsx and .xls files
    patterns = ["*.xlsx", "*.xls"]
    matched_files = []

    for pattern in patterns:
        matched_files += [
            f for f in glob.glob(os.path.join(folder_path, pattern))
            if keyword.lower() in os.path.basename(f).lower() and "-cut" not in os.path.basename(f).lower()
        ]

    # If matching files exist, return the latest one
    if matched_files:
        latest_file = max(matched_files, key=os.path.getmtime)
        print(f"Automatically selected latest file (no -cut): {latest_file}")
        return latest_file
    else:
        # Fallback to manual selection
        print("No matching non-'-cut' file found. Please select manually.")
        return select_excel_file(name_for_manual)
        
def select_DB_file(name):
    root = tk.Tk()
    root.title(f"Select specific file : {name}")
    root.withdraw()
    print(f"Select the Database file: {name}")
    file_path = askopenfilename(filetypes=[("Database files", "*.db")])
    if not file_path:
        print("No file selected.")
        exit()
    return file_path  
 
def get_latest_DB_file(folder_path, keyword, name_for_manual="DB file"):
    # Search for .db files
    patterns = ["*.db"]
    matched_files = []

    for pattern in patterns:
        matched_files += [
            f for f in glob.glob(os.path.join(folder_path, pattern))
            if keyword.lower() in os.path.basename(f).lower()
        ]

    # If matching files exist, return the latest one
    if matched_files:
        latest_file = max(matched_files, key=os.path.getmtime)
        print(f"Automatically selected latest DB file : {latest_file}")
        return latest_file
    else:
        # Fallback to manual selection
        print("No matching non-'-cut' file found. Please select manually.")
        return select_DB_file(name_for_manual)
        
# === CONFIG ===
db_folder = r"C:\Pam_card\system_transform\database"
db_key = "Debtor_system"
db_path = get_latest_DB_file(db_folder,db_key,name_for_manual="Main_database")
table_name = 'Debtor_transaction'
transaction_folder = r"Z:\Alpha\Programing_source\Transaction\Summary"
transaction_file_key = "summary_data_file_"
sheet_name = 'transaction_record'
excel_path = get_latest_excel_file(transaction_folder, transaction_file_key, name_for_manual="summary_data_file_")

# Define expected Excel columns and mapping to DB
excel_columns = [
    'pam_code', 'status', 'mode', 'responsible', 'OA',
    'vat_principal', 'int_principal', 'DOC_NO', 'code',
    'channel', 'TR_Date', 'Pay_Date', 'EFF_Date',
    'Cash_inflow', 'all_sold_expense', 'Asset_value', 'sold_method', 'Discount'
]

column_mapping = {
    'pam_code': 'pam_code',
    'status': 'status',
    'mode': 'mode',
    'responsible': 'responsible',
    'OA': 'OA',
    'vat_principal': 'vat_principal',
    'int_principal': 'int_principal',
    'DOC_NO': 'DOC_NO',
    'code': 'code',
    'channel': 'channel',
    'TR_Date': 'TR_Date',
    'Pay_Date': 'Pay_Date',
    'EFF_Date': 'EFF_Date',
    'all_sold_expense': 'Additional',
    'sold_method': 'Note'
    # Combine these into Amount manually
}

unique_keys = ['pam_code', 'code', 'TR_Date', 'Pay_Date', 'EFF_Date', 'Amount']

# === STEP 1: Connect to DB and read existing data ===
conn = sqlite3.connect(db_path)
existing_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

# === STEP 2: Load Excel and process ===
excel_df = pd.read_excel(excel_path, sheet_name=sheet_name, usecols=excel_columns)

excel_df['TR_Date'] = pd.to_datetime(excel_df["TR_Date"], errors="coerce")
excel_df['Pay_Date'] = pd.to_datetime(excel_df["Pay_Date"], errors="coerce")
excel_df['EFF_Date'] = pd.to_datetime(excel_df["EFF_Date"], errors="coerce")

excel_df['TR_Date'] = excel_df['TR_Date'].dt.strftime('%d-%b-%y')
excel_df['Pay_Date'] = excel_df['Pay_Date'].dt.strftime('%d-%b-%y')
excel_df['EFF_Date'] = excel_df['EFF_Date'].dt.strftime('%d-%b-%y')

# Combine amount-related fields into a single 'Amount' column
excel_df['Amount'] = excel_df[['Cash_inflow', 'Asset_value', 'Discount']].fillna(0).sum(axis=1)

# Drop original amount parts
excel_df.drop(columns=['Cash_inflow', 'Asset_value', 'Discount'], inplace=True)
excel_df = excel_df[excel_df['code'] != 'aq'].reset_index(drop=True)

# Rename to match DB columns
excel_df.rename(columns=column_mapping, inplace=True)

# === STEP 3: Remove already existing records ===
excel_df['pam_code'] = excel_df['pam_code'].astype(str).str.strip()
existing_df['pam_code'] = existing_df['pam_code'].astype(str).str.strip()
excel_df['Amount'] = excel_df['Amount'].astype(float)
existing_df['Amount'] = existing_df['Amount'].astype(float)

merged_df = pd.merge(excel_df, existing_df, on=unique_keys, how='left', indicator=True)
new_records = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

# Drop DB-suffixed columns if any exist (from merge)
new_records = new_records[[col for col in new_records.columns if not col.endswith('_y')]]
new_records.columns = [col.replace('_x', '') for col in new_records.columns]

# === STEP 4: Insert new records ===
if existing_df.empty:
    new_records = excel_df.copy()
    
if not new_records.empty:
    new_records.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"{len(new_records)} new records added to {table_name}.")
else:
    print("No new records to add.")

# === STEP 5: Close connection ===
conn.commit()
conn.close()
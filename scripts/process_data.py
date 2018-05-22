"""
Import the source xlsx file and create a database per the question format
"""

import pandas as pd
import sqlite3
import os

def xlsx_to_db(data_path, input_filename, db_name="SalesData.sqlite"):
    """Take an Excel file and turn it into a sqlite database

    Notes
    ----
    Data structure is derived from question asked. The most productionized
    way of doing this would involve packages like sqlalchemy but for a
    one time ask Pandas is the quickest

    
    Parameters
    ----------
    data_path: str
        Path to data directory

    input_filename: str
        Name of input excel sheet

    db_name: str
        Name of output database 

    Returns
    -------
    None
    """
    # Read in Excel Sheet
    df = pd.read_excel(os.path.join(data_path, input_filename))
    
    # Calculate total price per row
    df["Total"] = df["Quantity"] * df["UnitPrice"]

    # Filter out customers that are null. In many cases null values should
    # be investigated since they could indicate data quality issues
    # but for expediencies sake they'll be removed from this dataset

    df_filtered = df[~df["CustomerID"].isnull()]
    
    # Assume negative quantity is a return and label as such
    df_filtered["Transaction_Type"] = df_filtered["Quantity"] \
                    .apply(lambda x: "Sale" if x > 0 else "Return")

    # Collapse line level information into customer information
    dimensions = ["InvoiceNo", "InvoiceDate",  "CustomerID", "Transaction_Type"]
    facts = ["Total", "Quantity"]

    df_agg = df_filtered[facts+dimensions].groupby(dimensions).sum()
    df_agg.reset_index(inplace=True)
    
    # Convert CustomerID into an int
    df_agg["CustomerID"] = df_agg["CustomerID"].astype(int)

    # Check that all sale values are positive to ensure data cleanup worked
    assert (df_agg["Total"][df_agg["Transaction_Type"] == "Sale"] >= 0).all() == True

    # Rename columns to match prompt
    new_cols = ["sale_id", "sale_date", "buyer_email_address", "transaction_type", "total_usd", "num_items"]
   
    # Create remapping dictionary
    mapped_names = {old_col:new_col for old_col, new_col in zip((dimensions+facts), new_cols)}
    df_agg.columns = [mapped_names[col] for col in (dimensions+facts)]

    conn = sqlite3.connect(os.path.join(data_path, db_name))
    df_agg.to_sql("sales", conn, if_exists="replace")
    return


if __name__ == "__main__":
    xlsx_to_db("../data", "Online Retail.xlsx")

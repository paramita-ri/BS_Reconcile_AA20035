import tkinter as tk
from tkinter import  ttk, messagebox, filedialog
import pandas as pd
from datetime import datetime  # Fixed typo here (was 'form datetime')
import os
import numpy as np
import calendar



def readAAFeb():
    aafeb = pd.read_excel("/Users/peposeemuang/Desktop/BS_Reconcile/ReconcileFileMars/AAFeb.xlsx"
    , dtype={'Cost Center': str, "LOB": str}, sheet_name="Sheet1")
    return aafeb

def readReconcile():
    temp_df = pd.read_excel("/Users/peposeemuang/Desktop/BS_Reconcile/ReconcileFileMars/reconcileMar.xlsx"
    , sheet_name="Recap-Feb-25")
    start_row = None
    for i, row in temp_df.iterrows():
        if "Preroid" in str(row.values):
            start_row = i
            break
    if start_row is None:
        raise ValueError("Cannot find correct header row in Reconcile file.")
    LastReconcile_df = pd.read_excel("/Users/peposeemuang/Desktop/BS_Reconcile/ReconcileFileMars/reconcileMar.xlsx"
    , dtype={'Cost Center': str, "LOB": str}, sheet_name="Recap-Feb-25", header=start_row + 1)
    LastReconcile_df = LastReconcile_df.rename(columns={'Preroid': 'Period'})
    LastReconcile_df = LastReconcile_df.rename(columns={' Status/Reported GTO No.': 'Status/Reported GTO No.'})

    return LastReconcile_df

def standardize_period_dates(df):
    """
    Convert all dates in the Period column to dd/mm/yyyy format
    """
    # First convert everything to datetime objects
    df['Period'] = pd.to_datetime(
        df['Period'],
        errors='coerce',  # Convert invalid dates to NaT
        format='%b-%y',  # Try MMM-YY format first
        exact=False  # Allow partial matches
    )
    
    # For any remaining NaT values, try other formats
    df['Period'] = df['Period'].fillna(
        pd.to_datetime(df['Period'], errors='coerce', format='%Y-%m-%d')
    )
    
    # Now convert all valid dates to dd/mm/yyyy string format
    df['Period'] = df['Period'].dt.strftime('%d/%m/%Y')
    
    df['Transaction Date'] = pd.to_datetime(
            df['Transaction Date'],
            format='%d/%m/%Y %I:%M:%S %p',
            errors='coerce'
        ).dt.strftime('%d/%m/%Y')
    
    return df


def save_to_excel(df):
    """Save DataFrame to Excel"""
    save_path = filedialog.asksaveasfilename(
    title="Save the File",
    defaultextension=".xlsx",
    filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
    )
        
    if save_path:
        try:
            df.to_excel(save_path, index=False)
            messagebox.showinfo("Success", f"File saved successfully at {save_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save file. Error: {str(e)}")
            
if __name__ == "__main__":
    #aafeb = readAAFeb()
    reconcile = readReconcile()
    reconcile_df = standardize_period_dates(reconcile)
    #save_to_excel(aafeb)
    save_to_excel(reconcile_df)
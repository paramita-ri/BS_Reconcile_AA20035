import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
from datetime import datetime
import numpy as np

class GetReport():
    def __init__(self, PendingBills_df, Minimum_df):
        self.PendingBills_df = PendingBills_df
        self.Minimum_df = Minimum_df
        self.NewReconcile = None
        self.Groupby_df = None
        self.OnlyPending = None
        self.OnlyMinimum = None

    def getNewReconcile(self):
        if self.PendingBills_df is None or self.Minimum_df is None:
            return None
        pendingBills = self.PendingBills_df.copy()
        Minimum_df = self.Minimum_df.copy()
        NewReconcile = pd.concat([pendingBills, Minimum_df], ignore_index=True)
        NewReconcile['Cost Center'] = NewReconcile['Cost Center'].astype(str).str.zfill(5)
    

        NewReconcile['Transaction Date'] = pd.to_datetime(
            NewReconcile['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )
        NewReconcile = NewReconcile.sort_values(by=['Cost Center', 'Transaction Date'])
    
        NewReconcile['Transaction Date'] = NewReconcile['Transaction Date'].dt.strftime('%d/%m/%Y')
        NewReconcile = NewReconcile.reset_index(drop= True) 
        self.NewReconcile = NewReconcile
        return self.NewReconcile

    def getGroupby(self):
        if self.NewReconcile is None:
            self.getNewReconcile()
        df = self.NewReconcile.copy()

        if 'Group' not in df.columns or 'Transaction Date' not in df.columns or 'Total' not in df.columns:
            return None

        df['Transaction Date'] = pd.to_datetime(
            df['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )

        pivot_df = pd.pivot_table(
            df,
            index='Group',
            columns=pd.Grouper(key='Transaction Date', freq='YE'),
            values='Total',
            aggfunc='sum',
            fill_value=0
        )

        pivot_df.columns = pivot_df.columns.year
        pivot_df = pivot_df.reset_index()
        self.Groupby_df = pivot_df
        return self.Groupby_df

    def getOnlyPending(self):
        if self.NewReconcile is None:
            self.getNewReconcile()
        df = self.NewReconcile.copy()

        if 'Group' not in df.columns:
            return None

        pattern = r'^ยอดขาย Food Court.*ที่ลูกค้ายังไม่มาวางบิล'
        filtered_df = df[df['Group'].str.contains(pattern, regex=True, case=False, na=False)]
        self.OnlyPending = filtered_df.reset_index(drop=True)
        return self.OnlyPending

    def getOnlyMinimum(self):
        if self.NewReconcile is None:
            self.getNewReconcile()
        df = self.NewReconcile.copy()

        if 'Group' not in df.columns:
            return None
        mask = df['Group'].str.contains("Minimum Guarantee", case=False, na=False)
        minimum_guarantee_df = df[mask].copy()
        self.OnlyMinimum = minimum_guarantee_df.reset_index(drop=True)
        return self.OnlyMinimum

    def save_to_excel(self, dfs: dict, title: str, default_name: str):
        """Save multiple DataFrames into one Excel file with separate sheets."""
        save_path = filedialog.asksaveasfilename(
            title=title,
            initialfile=default_name,
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )

        if save_path:
            try:
                with pd.ExcelWriter(save_path, engine='xlsxwriter') as writer:
                    for sheet_name, df in dfs.items():
                        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                messagebox.showinfo("Success", f"File saved successfully:\n{save_path}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save file:\n{str(e)}")


    def getReport(self):
        newReconcile = self.getNewReconcile()
        groupby = self.getGroupby()
        onlypending = self.getOnlyPending()
        onlyminimum = self.getOnlyMinimum()

        all_dfs = {}
        if newReconcile is not None:
            all_dfs["New Reconcile"] = newReconcile
        if groupby is not None:
            all_dfs["Groupby Summary"] = groupby
        if onlypending is not None:
            all_dfs["Only Pending"] = onlypending
        if onlyminimum is not None:
            all_dfs["Only Minimum Guarantee"] = onlyminimum

        if all_dfs:
            self.save_to_excel(all_dfs, "Save All Reports", "ReconcileReport.xlsx")


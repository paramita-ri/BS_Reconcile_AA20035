from tkinter import filedialog, messagebox
import numpy as np
import pandas as pd
import threading

class GetReport():
    def __init__(self, PendingBills_df, Minimum_df, AAStar , GTO05, parent_app=None):
        self.PendingBills_df = PendingBills_df
        self.Minimum_df = Minimum_df
        self.parent_app = parent_app
        self.AAstar_df = AAStar
        self.GTO05_df = GTO05
        self.MiniGTO_df = None
        self.NewReconcile = None
        self.Groupby_df = None
        self.OnlyPending = None
        self.OnlyMinimum = None
        self.save_complete = threading.Event()
        self.save_path = None
    
    def log_message(self, message):
        if self.parent_app:
            self.parent_app.log_message(message)
    
    def update_progress(self, value, message):
        if self.parent_app:
            self.parent_app.update_progress(value, message)

    def getMiniGTO_df(self, Newreconcile):
        
        reconcile = Newreconcile.copy()
        aastar = self.AAstar_df.copy()
        gto05 = self.GTO05_df.copy()
        #simplifie column name in aastar and gto05
        aastar = aastar.rename(columns={
            'MRI or Anacle TransactionID':'TS_ID',
        })
        gto05 = gto05.rename(columns={
            'Reported GTO No.':'TS_ID',
            'Customer':'Vendor',
            'COGS Refund':'Refund',
            'Adjusted MMG GP Amount':'MMG'
        })
        #drop row that has nan in critical column and left only column 
        #we want in aastar and gto05
        aastar = aastar.dropna(subset=['TS_ID'])[['Cost Center', 'Transaction Date', 'TS_ID']]
        gto05 = gto05[['TS_ID', 'Vendor', 'Refund','MMG']]
        reconcile = reconcile[['Cost Center', 'Transaction Date']]
        
        # First get TS_IDs from aastar
        merged_aastar = pd.merge(reconcile, aastar, on=['Cost Center', 'Transaction Date'], how='left')
        merged_aastar = merged_aastar.dropna(subset=['TS_ID'])

        # Now get TS_IDs from Newreconcile if already present
        existing_ts_id_rows = Newreconcile.dropna(subset=['Status/Reported GTO No.'])[
            ['Cost Center', 'Transaction Date', 'Status/Reported GTO No.']
        ].rename(columns={'Status/Reported GTO No.': 'TS_ID'})

        # Combine both
        mergeID_df = pd.concat([merged_aastar[['Cost Center', 'Transaction Date', 'TS_ID']], existing_ts_id_rows], ignore_index=True)
        mergeID_df = mergeID_df.drop_duplicates()

        
        mergeMiniGTO_df = pd.merge(mergeID_df, gto05, on=['TS_ID'], how='left')
        mergeMiniGTO_df = mergeMiniGTO_df.dropna(subset=['Refund','MMG'])
        #Drop rows where both Refund and MMG are 0 or NaN
        mergeMiniGTO_df = mergeMiniGTO_df[
            ~((mergeMiniGTO_df['Refund'].fillna(0) == 0) & (mergeMiniGTO_df['MMG'].fillna(0) == 0))
        ]
        self.MiniGTO_df = mergeMiniGTO_df
        return self.MiniGTO_df 
    
    def getNewReconcile(self):
        if self.PendingBills_df is None or self.Minimum_df is None:
            return None
        
        self.log_message("Creating new reconciliation report...")
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
        NewReconcile['Period'] = pd.to_datetime(
            NewReconcile['Period'],
            format='%d/%m/%Y', errors='coerce'
        )
        NewReconcile = NewReconcile.reset_index(drop=True) 
        miniGTO = self.getMiniGTO_df(NewReconcile)
        key_cols = ['Cost Center', 'Transaction Date']
        mapping_dict = miniGTO.set_index(key_cols)['TS_ID'].to_dict()

        def fill_status(row):
            if pd.isna(row.get('Status/Reported GTO No.')):
                key = (row['Cost Center'], row['Transaction Date'])
                return mapping_dict.get(key, np.nan)
            return row['Status/Reported GTO No.']

        NewReconcile['Status/Reported GTO No.'] = NewReconcile.apply(fill_status, axis=1)
        NewReconcile['Transaction Date'] = pd.to_datetime(
            NewReconcile['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )
        self.MiniGTO_df['Transaction Date'] = pd.to_datetime(
            self.MiniGTO_df['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )
        self.NewReconcile = NewReconcile
        self.log_message("New reconciliation report created")
        return self.NewReconcile

    def getGroupby(self):
        if self.NewReconcile is None:
            self.getNewReconcile()
        df = self.NewReconcile.copy()

        if 'Group' not in df.columns or 'Transaction Date' not in df.columns or 'Total' not in df.columns:
            return None

        self.log_message("Creating Summary Current Month...")
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
        self.log_message("Summary Current Month created")
        return self.Groupby_df

    def getOnlyPending(self):
        if self.NewReconcile is None:
            self.getNewReconcile()
        df = self.NewReconcile.copy()

        if 'Group' not in df.columns:
            return None

        self.log_message("Filtering pending bills...")
        pattern = r'^ยอดขาย Food Court.*ที่ลูกค้ายังไม่มาวางบิล'
        filtered_df = df[df['Group'].str.contains(pattern, regex=True, case=False, na=False)]
        self.OnlyPending = filtered_df.reset_index(drop=True)
        self.log_message("Pending bills filtered")
        return self.OnlyPending

    def getOnlyMinimum(self):
        if self.NewReconcile is None:
            self.getNewReconcile()
        df = self.NewReconcile.copy()

        if 'Group' not in df.columns:
            return None
        
        self.log_message("Filtering minimum guarantee...")
        mask = df['Group'].str.contains("Minimum Guarantee", case=False, na=False)
        minimum_guarantee_df = df[mask].copy()
        self.OnlyMinimum = minimum_guarantee_df.reset_index(drop=True)
        self.log_message("Minimum guarantee filtered")
        return self.OnlyMinimum

    def _save_to_excel_thread(self, dfs, title, default_name):
        """This runs in the main thread"""
        save_path = filedialog.asksaveasfilename(
            parent=self.parent_app.root,
            title=title,
            initialfile=default_name,
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )

        if save_path:
            try:
                self.log_message(f"Saving report to: {save_path}")
                # Create Excel writer with nan_inf_to_errors option
                with pd.ExcelWriter(
                    save_path, 
                    engine='xlsxwriter',
                    engine_kwargs={'options': {'nan_inf_to_errors': True}}
                ) as writer:
                    workbook = writer.book
                    
                    # Define formats
                    header_format = workbook.add_format({
                        'bold': True,
                        'text_wrap': True,
                        'valign': 'top',
                        'fg_color': '#D7E4BC',
                        'border': 1
                    })
                    
                    cell_border_format = workbook.add_format({'border': 1})
                    
                    # Format for NaN/INF values
                    nan_format = workbook.add_format({
                        'border': 1,
                        'font_color': '#808080'  # Gray color for NaN/INF
                    })
                    
                    date_format_display = workbook.add_format({
                        'num_format': 'dd-mmm-yy',
                        'border': 1
                    })
                    
                    period_format_display = workbook.add_format({
                        'num_format': 'mmm-yy',
                        'border': 1
                    })
                    
                    accounting_format = workbook.add_format({
                        'num_format': '#,##0.00_);(#,##0.00)',
                        'border': 1
                    })
                    
                    accounting_format_red = workbook.add_format({
                        'num_format': '#,##0.00_);(#,##0.00)',
                        'font_color': 'red',
                        'border': 1
                    })

                    for sheet_name, df in dfs.items():
                        # Replace INF with NaN then fill NaN with empty string for display
                        clean_df = df.replace([np.inf, -np.inf], np.nan)
                        
                        # Write the cleaned dataframe to Excel
                        clean_df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                        
                        worksheet = writer.sheets[sheet_name[:31]]
                        
                        # Set column widths and header format
                        for col_num, value in enumerate(clean_df.columns.values):
                            worksheet.set_column(col_num, col_num, max(len(str(value)) + 2, 12))
                            worksheet.write(0, col_num, value, header_format)
                        
                        # Apply cell formatting
                        for row in range(1, len(clean_df)+1):
                            for col in range(0, len(clean_df.columns)):
                                cell_value = clean_df.iloc[row-1, col]
                                
                                # Check for NaN/INF first
                                if pd.isna(cell_value):
                                    worksheet.write(row, col, "", nan_format)
                                    continue
                                
                                # Special formatting for specific sheets
                                if sheet_name in ["Reconcile Report", "Food Courts Pending Bills", "Minimum Guarantee", "Refund & Minimum"]:
                                    col_name = clean_df.columns[col]
                                    
                                    if 'Transaction Date' in str(col_name):
                                        try:
                                            worksheet.write_datetime(row, col, pd.to_datetime(cell_value), date_format_display)
                                        except:
                                            worksheet.write(row, col, cell_value, cell_border_format)
                                    elif 'Period' in str(col_name):
                                        try:
                                            worksheet.write_datetime(row, col, pd.to_datetime(cell_value), period_format_display)
                                        except:
                                            worksheet.write(row, col, cell_value, cell_border_format)
                                    elif 'Total' in str(col_name):
                                        try:
                                            num_value = float(cell_value)
                                            if num_value < 0:
                                                worksheet.write(row, col, num_value, accounting_format_red)
                                            else:
                                                worksheet.write(row, col, num_value, accounting_format)
                                        except (ValueError, TypeError):
                                            worksheet.write(row, col, cell_value, cell_border_format)
                                    elif 'MMG' in str(col_name):
                                        try:
                                            num_value = float(cell_value)
                                            if num_value < 0:
                                                worksheet.write(row, col, num_value, accounting_format_red)
                                            else:
                                                worksheet.write(row, col, num_value, accounting_format)
                                        except (ValueError, TypeError):
                                            worksheet.write(row, col, cell_value, cell_border_format)
                                    elif 'Refund' in str(col_name):
                                        try:
                                            num_value = float(cell_value)
                                            if num_value < 0:
                                                worksheet.write(row, col, num_value, accounting_format_red)
                                            else:
                                                worksheet.write(row, col, num_value, accounting_format)
                                        except (ValueError, TypeError):
                                            worksheet.write(row, col, cell_value, cell_border_format)
                                    else:
                                        worksheet.write(row, col, cell_value, cell_border_format)
                                else:
                                    worksheet.write(row, col, cell_value, cell_border_format)
                        
                        # Freeze the header row and add autofilter
                        worksheet.freeze_panes(1, 0)
                        worksheet.autofilter(0, 0, len(clean_df), len(clean_df.columns)-1)
                        
                        self.log_message(f"Saved sheet: {sheet_name}")
                
                self.save_path = save_path
                messagebox.showinfo("Success", f"File saved successfully:\n{save_path}")
            except Exception as e:
                self.save_path = None
                messagebox.showerror("Error", f"Failed to save file:\n{str(e)}")
        
        self.save_complete.set()

    def save_to_excel(self, dfs, title, default_name):
        """Thread-safe file saving"""
        self.save_complete.clear()
        self.save_path = None
        
        # Schedule the save dialog to run in the main thread
        self.parent_app.root.after(0, lambda: self._save_to_excel_thread(dfs, title, default_name))
        
        # Wait for the save operation to complete
        self.save_complete.wait()
        
        return self.save_path

    def getReport(self):
        self.update_progress(95, "Preparing reports...")
        
        newReconcile = self.getNewReconcile()
        groupby = self.getGroupby()
        onlypending = self.getOnlyPending()
        onlyminimum = self.getOnlyMinimum()
        MiniGTO_df = self.MiniGTO_df



        all_dfs = {}
        if newReconcile is not None:
            all_dfs["Reconcile Report"] = newReconcile
        if groupby is not None:
            all_dfs["Summary Current Month"] = groupby
        if onlypending is not None:
            all_dfs["Food Courts Pending Bills"] = onlypending
        if onlyminimum is not None:
            all_dfs["Minimum Guarantee"] = onlyminimum
        if MiniGTO_df is not None:
            all_dfs["Refund & Minimum"] = MiniGTO_df

        if all_dfs:
            saved_path = self.save_to_excel(all_dfs, "Save All Reports", "ReconcileReport.xlsx")
            if saved_path:
                self.log_message(f"Reports successfully saved to: {saved_path}")
            else:
                self.log_message("Report saving was cancelled")
        
        self.update_progress(100, "Reports generation complete")
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
import numpy as np
from datetime import datetime
import queue

class GetFileAndPeriod():
    def __init__(self, parent_app=None):
        self.parent_app = parent_app
        self.Period_date = None        
        self.AACurrent_df = None       
        self.AAPrevious_df = None      
        self.AAStar_df = None             
        self.LastReconcile_df = None  
        self.GTO05_df = None           
        self.file_paths = {          
            "AAThismonth": None,
            "AALastmonth": None,
            "ReconcileLastMonth": None,
            "GTO05": None,
        }
        self.period_selected = threading.Event()
        self.files_selected = threading.Event()
        self.result_queue = queue.Queue()

    def log_message(self, message):
        if self.parent_app:
            self.parent_app.log_message(message)

    def update_progress(self, value, message):
        if self.parent_app:
            self.parent_app.update_progress(value, message)

    def getPeriod(self):
        # Request the main thread to show period dialog
        self.parent_app.root.after(0, self._show_period_dialog)
        
        # Wait for the user to select the period
        self.period_selected.wait()
        self.period_selected.clear()
        
        # Get the result from the queue
        if not self.result_queue.empty():
            self.Period_date = self.result_queue.get()
            self.log_message(f"Selected period: {self.Period_date}")
        
        return self.Period_date

    def _show_period_dialog(self):
        """This runs in the main thread"""
        def on_submit():
            month = month_var.get()
            year = year_var.get()
            try:
                date_obj = datetime.strptime(f"{month} {year}", "%B %Y").date()
                self.Period_date = date_obj.strftime("%d/%m/%Y")
                self.result_queue.put(self.Period_date)
                root.destroy()
                self.period_selected.set()
            except Exception as e:
                messagebox.showerror("Error", f"Invalid date selection: {str(e)}")

        root = tk.Toplevel(self.parent_app.root)
        root.title("Select Period")
        root.transient(self.parent_app.root)
        root.grab_set()

        # Center the window
        window_width = 300
        window_height = 200
        root.geometry(f'{window_width}x{window_height}+{int(root.winfo_screenwidth()/2-window_width/2)}+{int(root.winfo_screenheight()/2-window_height/2)}')
        root.resizable(False, False)

        # Month dropdown
        months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        current_month = datetime.now().month - 1
        month_var = tk.StringVar(value=months[current_month])
        ttk.Label(root, text="Select Month:").pack(pady=(10, 0))
        month_dropdown = ttk.Combobox(root, textvariable=month_var, values=months, state="readonly")
        month_dropdown.pack(pady=5)

        # Year dropdown
        current_year = datetime.now().year
        years = [str(year) for year in range(current_year - 2, current_year + 3)]
        year_var = tk.StringVar(value=str(current_year))
        ttk.Label(root, text="Select Year:").pack(pady=(10, 0))
        year_dropdown = ttk.Combobox(root, textvariable=year_var, values=years, state="readonly")
        year_dropdown.pack(pady=5)

        # Submit button
        submit_btn = ttk.Button(root, text="Submit", command=on_submit)
        submit_btn.pack(pady=10)

    def getFilePath(self):
        # Show instructions in main thread
        self.parent_app.root.after(0, self._show_file_dialog_instructions)
        
        # Wait for files to be selected
        self.files_selected.wait()
        self.files_selected.clear()

    def _show_file_dialog_instructions(self):
        """This runs in the main thread"""
        instructions = (
            "Please select 4 Excel files in this order:\n"
            "1. AA20035 This Month\n"
            "2. AA20035 Last Month\n"
            "3. Reconcile Last Month\n"
            "4. GTO05 File"
        )
        messagebox.showinfo("Instructions", instructions)
        
        # Start file selection process
        self.parent_app.root.after(0, self._select_files)

    def _select_files(self):
        """This runs in the main thread"""
        file_order = ["AAThismonth", "AALastmonth", "ReconcileLastMonth", "GTO05"]
        
        for i, file_key in enumerate(file_order):
            self.update_progress(20 + i*5, f"Selecting {file_key.replace('_', ' ')} file...")
            
            file_path = filedialog.askopenfilename(
                parent=self.parent_app.root,
                title=f"Select file for {file_key}",
                filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
            )
            
            if not file_path:
                messagebox.showerror("Error", "You must select all 4 files. Process cancelled.")
                self.result_queue.put(Exception("File selection cancelled."))
                return
            
            self.file_paths[file_key] = file_path
            self.log_message(f"Selected file for {file_key}: {file_path}")
        
        self.files_selected.set()

    def getFile(self):
        self.getFilePath()
        
        # Check if we got an error
        if not self.result_queue.empty():
            error = self.result_queue.get()
            if isinstance(error, Exception):
                raise error
        
        self.update_progress(40, "Loading AA Current Month file...")
        self.AACurrent_df = pd.read_excel(
            self.file_paths["AAThismonth"],                      
            dtype={'Cost Center': str, 'LOB': str}
        )
        
        self.update_progress(50, "Loading AA Last Month file...")
        self.AAPrevious_df = pd.read_excel(
            self.file_paths["AALastmonth"],
            dtype={'Cost Center': str, 'LOB': str}
        )
        
        self.update_progress(60, "Loading Last Reconciliation file...")
        self.LastReconcile_df = pd.read_excel(
            self.file_paths['ReconcileLastMonth'], 
            dtype={'Cost Center': str, 'LOB': str}
        )
        
        self.update_progress(70, "Loading GTO05 file...")
        self.read_GTO05()
        
        self.update_progress(80, "Cleaning dates...")
        self.CleanDate(self.AACurrent_df)
        self.CleanDate(self.AAPrevious_df)
        
        self.update_progress(90, "Creating AA Star data...")
        self.getAAStar()
        
        self.log_message("All input files loaded successfully")
        return self.AACurrent_df, self.AAStar_df, self.LastReconcile_df, self.GTO05_df

    
    def read_GTO05(self):
        findTable_df = pd.read_excel(self.file_paths["GTO05"], nrows=20)
        start_row = None
     
        for i, row in findTable_df.iterrows():
            if all(col in str(row.values) for col in ["Cost Center Name", "Building"]):
                start_row = i
                break
        if start_row is None:
            raise Exception("Cannot find correct header row in GTO05 file.")
  
        self.GTO05_df = pd.read_excel(
            self.file_paths["GTO05"],
            dtype={'Cost Center Number': str, 'LOB': str},
            header=start_row + 1
        )
    
    def CleanDate(self, df):
        df['Gl_Date'] = pd.to_datetime(
            df['Gl_Date'],
            format='%d/%m/%Y %I:%M:%S %p',
            errors='coerce'
        ).dt.strftime('%d/%m/%Y')
        
        df['Transaction Date'] = pd.to_datetime(
            df['Transaction Date'],
            format='%d-%b-%y',
            errors='coerce'
        ).dt.strftime('%d/%m/%Y')

        df['Transaction Date'] = np.where(
            df['Transaction Date'].isna() | (df['Transaction Date'] == ''),
            df['Gl_Date'],
            df['Transaction Date']
        )
    
    def getAAStar(self):
        aaPrevious = self.AAPrevious_df.copy()
        aaCurrent = self.AACurrent_df.copy()

        merged_df = pd.concat([aaPrevious, aaCurrent], ignore_index=True)

        merged_df['Transaction Date'] = pd.to_datetime(
            merged_df['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )

        sorted_df = merged_df.sort_values(by=['Cost Center', 'Transaction Date'])
        sorted_df['Transaction Date'] = sorted_df['Transaction Date'].dt.strftime('%d/%m/%Y')

        sorted_df.reset_index(drop=True, inplace=True)
        self.AAStar_df = sorted_df
        
    def getNewReconcileFile(self):
        # Ask user to select a file
        file_path = filedialog.askopenfilename(
            parent=self.parent_app.root,
            title="Select Reconcile Report file",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )

        if not file_path:
            messagebox.showerror("Error", "No file selected. Process cancelled.")
            raise Exception("Reconcile file selection cancelled.")

        self.newReconcile = file_path
        self.log_message(f"Selected New Reconcile file: {file_path}")

        try:
            self.newReconcile_df = pd.read_excel(
                file_path,
                sheet_name="Reconcile Report",
                dtype={'Cost Center': str, 'LOB': str}
            )
            self.log_message("New Reconcile file loaded successfully.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read the 'Reconcile Report' sheet: {str(e)}")
            raise

"""
Class เพื่อรับ input period และ load excel file ไปยัง data frame 
return ค่า 
    1.Period_date 
    2.AACurrent_df
    3.AAPrevious_df 
    4.AAStar_df 
    5.LastReconcile_df 
    6.GTO05_df 
"""
import tkinter as tk
from tkinter import  ttk, messagebox, filedialog
import pandas as pd
import numpy as np
from datetime import datetime

class GetFileAndPeriod():
    
    def __init__(self):
        self.Period_date = None         # ตัวแปรเก็บ period ปัจจุบัน
        self.AACurrent_df = None        # dataframe ของ AA20035 เดือนปัจจุบัน
        self.AAPrevious_df = None       # dataframe ของ AA20035 เดือนก่อนหน้า
        self.AAStar_df = None             # dataframe รวม AA20035 สองเดือนเพื่อหา minimum
        self.LastReconcile_df = None    # dataframe ของ reconcile File เดือนก่อนหน้า
        self.GTO05_df = None            # dataframe ของ GTO05
        """self.file_paths = {             # ตัวแปรเพื่อเก็บ file path ของ excel
            "AAThismonth": "/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/InputFile/AACurrentMonth.xlsx",
            "AALastmonth": "/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/InputFile/AAPrevious.xlsx",
            "ReconcileLastMonth": "/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/InputFile/LastReconcile.xlsx",
            "GTO05": "/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/InputFile/GTO05.xlsx",
        }
        """
        self.file_paths = {             # ตัวแปรเพื่อเก็บ file path ของ excel
            "AAThismonth": None,
            "AALastmonth": None,
            "ReconcileLastMonth": None,
            "GTO05": None,
        }
        
    """
    function เพื่อรับค่า period ปัจจุบันจาก user
    """
    def getPeriod(self):
        # สร้าง Tkinter window เพื่อเลือก period
        def on_submit():
            month = month_var.get()
            year = year_var.get()
            date_obj = datetime.strptime(f"{month} {year}", "%B %Y").date()
            self.Period_date = date_obj.strftime("%d/%m/%Y")  # Format as dd/mm/yyyy
            root.quit()
            root.destroy()
            
        # สร้าง window
        root = tk.Tk()
        root.title("Select Period")

        # Set window size
        window_width = 300
        window_height = 200

        # Get the screen dimension
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()

        # Find the center point
        center_x = int(screen_width/2 - window_width/2)
        center_y = int(screen_height/2 - window_height/2)

        # Set the position of the window to the center
        root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
        root.resizable(False, False)

        # Month dropdown
        months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        month_var = tk.StringVar(value=months[0])  # Default to January
        month_dropdown = ttk.Combobox(root, textvariable=month_var, values=months, state="readonly")
        month_dropdown.current(0)
        month_dropdown.pack(pady=10)

        # Year dropdown
        years = [str(year) for year in range(2023, 2031)]
        year_var = tk.StringVar(value="2025")
        year_dropdown = ttk.Combobox(root, textvariable=year_var, values=years, state="readonly")
        year_dropdown.current(years.index("2025"))
        year_dropdown.pack(pady=10)

        # Button to collect and print the selected period
        submit_btn = ttk.Button(root, text="Submit", command=on_submit)
        submit_btn.pack(pady=10)

        root.mainloop()
        
        return self.Period_date # return ค่า period date ใน format dd/mm/yyyy
    
    def getFilePath(self):

        # Show instructions
        instructions = (
            "Please select 4 Excel files in this order:\n"
            "1. AA20035 This Month\n"
            "2. AA20035 Last Month\n"
            "3. Reconcile Last Month\n"
            "4. GTO05 File"
        )
        messagebox.showinfo("Instructions", instructions)

        root = tk.Tk()
        root.withdraw()  # Hide root window

        file_order = ["AAThismonth", "AALastmonth", "ReconcileLastMonth", "GTO05"]
        for file_key in file_order:
            file_path = filedialog.askopenfilename(
                title=f"Select file for {file_key}",
                filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
            )
            if not file_path:
                messagebox.showerror("Error", "You must select all 4 files. Process cancelled.")
                raise Exception("File selection cancelled.")
            self.file_paths[file_key] = file_path
        
        
    """
    funtion เพื่อโหลดข้อมูลจาก excel ไปที่ data frame แล้ว return 
    data frame ทั้งหมด ได้แก่
        self.Period_date 
        self.AACurrent_df
        self.AAPrevious_df 
        self.AAStar_df 
        self.LastReconcile_df 
        self.GTO05_df 
    """    
    def getFile(self):
        self.getFilePath()
        self.AACurrent_df = pd.read_excel(
        self.file_paths["AAThismonth"],                         # read excel AACurrent_df, AAPervious, Last Reconcile
        dtype={'Cost Center': str,
            'LOB': str}
        )
        self.AAPrevious_df = pd.read_excel(
            self.file_paths["AALastmonth"],
            dtype={'Cost Center': str, 'LOB': str}
            )
        self.LastReconcile_df = pd.read_excel(
            self.file_paths['ReconcileLastMonth'], 
            dtype={'Cost Center': str, 'LOB': str}
            )
        self.read_GTO05()                                       # เรียก function เพื่ออ่าน GTO05
        self.CleanDate(self.AACurrent_df)                       # เรียก function เพื่อจัดการกับ Transaction Date ของ AACurrent_df
        self.CleanDate(self.AAPrevious_df)                      # เรียก function เพื่อจัดการกับ Transaction Date ของ AAPrevious_df
        self.getAAStar()                                        # เรียก function เพื่อรวม AA เป็น AAStar

        return self.AACurrent_df, self.AAPrevious_df, self.AAStar_df, self.LastReconcile_df, self.GTO05_df
    
    """
    function เพื่อโหลดไฟล์ GTO ไปยัง dataframe โดยข้าม description ด้านบน 
    และดึงเอาข้อมูลเฉพาะส่วนตารางมา
    """
    def read_GTO05(self):
        findTable_df = pd.read_excel(self.file_paths["GTO05"], nrows=20)
        start_row = None
        #เช็คหาชื่อ column จาก 20 row แรกใน excel และบัญทึกไว้
        for i, row in findTable_df.iterrows():
            if all(col in str(row.values) for col in ["Cost Center Name", "Building"]):
                start_row = i
                break
        if start_row is None:
            print("Cannot find correct header row in GTO05 file.")
        #เริ่มอ่านไฟล์จาก row ที่มีชื่อ column อยู่    
        self.GTO05_df = pd.read_excel(
        self.file_paths["GTO05"],
            dtype={'Cost Center Number': str,'LOB': str},
            header=start_row + 1
        )
    
    """
    function จัด format ของ Transaction Date ให้ตรงการ และเติม 
    missing value ใน Column Transaction Date ด้วยค่าจาก Gl_Date
    """
    def CleanDate(self, df):
        # เปลี่ยน format ของวันที่ให้เหมือนกัน
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
        
        # เติม Transaction Date ที่ว่างด้วยวันที่จาก Gl_Date
        df['Transaction Date'] = np.where(
            df['Transaction Date'].isna() | (df['Transaction Date'] == ''),
            df['Gl_Date'],
            df['Transaction Date']
        )
    
    """
    function เพื่อรวมไฟล์ AA ทั้ง 2 เดือนเพื่อให้ในการหา minimum guarantee
    """   
    def getAAStar(self):
        aaPrevious = self.AAPrevious_df.copy()
        aaCurrent = self.AACurrent_df.copy()

        # Combine AA data from both months
        merged_df = pd.concat([aaPrevious, aaCurrent], ignore_index=True)

        # Convert Transaction Date to datetime (let pandas infer format)
        merged_df['Transaction Date'] = pd.to_datetime(
            merged_df['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )

        # Sort by Cost Center and Transaction Date
        sorted_df = merged_df.sort_values(by=['Cost Center', 'Transaction Date'])

        # Convert back to string format for output
        sorted_df['Transaction Date'] = sorted_df['Transaction Date'].dt.strftime('%d/%m/%Y')

        sorted_df.reset_index(drop=True, inplace=True)
        self.AAStar_df = sorted_df

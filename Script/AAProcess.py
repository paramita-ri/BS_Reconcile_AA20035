""" 
Class เพื่อ ทำ pivot table ของ AA20035 เดือนปันจุบัน
โดยหลัง pivot table โดยใช้ index คือ 'Cost Center', 'Transaction Date'
และทำการ sum Amount แล้วเติม Columns โดยมี Columns order คือ
column_order = [
        'Period', 'Cost Center', 'Building', 'LOB', 'Transaction Date',
        'Total', 'Group', 'Vendor', 'Status/Reported GTO No.'
] แล้ว return AApivot_df, self.AAForCombine_df 
"""
import pandas as pd
import numpy as np
import calendar
from datetime import datetime

class AAProcess():
    
    def __init__(self, AACurrent, GTO05, Period_Date):
        self.AACurrent_df  = AACurrent
        self.GTO05_df = GTO05
        self.Period_Date = Period_Date
        self.AAPivot_df = None
        self.AAForCombine_df = None
        
    def getAApivot(self):
        # ทำ pivot table Group by cost center, Transactiondate แล้ว sum Amount
        pivot_df = self.AACurrent_df.copy()
        pivot_df = pd.pivot_table(
            pivot_df,
            values='Amount',
            index=['Cost Center', 'Transaction Date'],
            aggfunc='sum'
        ).reset_index()

        # Rename 'Amount' เป็น 'Total'
        #pivot_df = pivot_df.rename(columns={'Amount': 'Total'})

        # Round 'Total' เป็นทศนิยม 2 ตำแหน่ง
        pivot_df['Amount'] = pivot_df['Amount'].round(2)

        # drop row ที่ Total เป็น 0 หรือ Nan
        pivot_df = pivot_df[
            (pivot_df['Amount'] != 0) & 
            (~pivot_df['Amount'].isna())
        ]
        self.AAPivot_df = pivot_df
        self.FillColumn() # เพิ่ม column และเติมค่าต่างๆ 
        self.AAPivot_df = self.AAPivot_df.reset_index(drop=True)
        self.AAForCombine_df = self.AAForCombine_df.reset_index(drop=True)

        return self.AAPivot_df ,self.AAForCombine_df
    
    def FillColumn(self):
    
        fillColumn_df = self.AAPivot_df.copy()
        aacurrent_df = self.AACurrent_df.copy()
        # เติม column period ด้วย period date
        fillColumn_df['Period'] = self.Period_Date

        # เพิ่ม column LOB แล้วเติมค่า 00023
        fillColumn_df['LOB'] = "00023"

        # เพิ่ม column 'Building'ด้วย mapping จาก GTO05_df
        # สร้่าง mapping dictionary จาก GTO_df
        building_mapping = self.GTO05_df.set_index('Cost Center Number')['Building Name'].to_dict()

        # Map Building values โดยใช้ Cost Center เป็น key
        fillColumn_df['Building'] = fillColumn_df['Cost Center'].map(building_mapping)
        # Step 1: Ensure raw_df has only one row per Cost Center + Date + Amount
        aacurrent_df_unique = aacurrent_df.drop_duplicates(subset=['Cost Center', 'Transaction Date', 'Amount'])

        # Step 2: Merge
        fillColumn_df = pd.merge(
            fillColumn_df,
            aacurrent_df_unique[['Cost Center', 'Transaction Date', 'Amount', 'Vendor / Customer Name']],
            on=['Cost Center', 'Transaction Date', 'Amount'],
            how='left'
        )


        # เพิ่ม columns ด้วย empty values
        fillColumn_df['Group'] = ""
        fillColumn_df['Status/Reported GTO No.'] = ""
        fillColumn_df['Remark'] = ""
        fillColumn_df['Question'] = ""
        
        fillColumn_df = fillColumn_df.rename(columns={'Amount': 'Total'})
        fillColumn_df = fillColumn_df.rename(columns={'Vendor / Customer Name': 'Vendor'})
        # define column order
        column_order = [
            'Period', 'Cost Center', 'Building', 'LOB', 'Transaction Date',
            'Total', 'Group', 'Vendor', 'Status/Reported GTO No.','Remark','Question'
        ]

        # Reorder the columns
        fillColumn_df = fillColumn_df[column_order]
        self.AAForCombine_df = fillColumn_df
        
           
import pandas as pd
import numpy as np
import calendar
from datetime import datetime

class Combine():
    def __init__(self, AAForcombine_df, LastReconcile_df):
        self.AAForCombine_df = AAForcombine_df
        self.LastReconcile_df = LastReconcile_df
        self.Combined_df = None
        
    def getCombine_df(self):
        aacombine = self.AAForCombine_df.copy()
        lastreconcile = self.LastReconcile_df.copy()
        
        column_order = [
            'Period', 'Cost Center', 'Building', 'LOB', 'Transaction Date',
            'Total', 'Group', 'Vendor', 'Status/Reported GTO No.','Remark','Question'
        ]
        lastreconcile = lastreconcile[column_order]
        
        CombineTable_df = pd.concat([lastreconcile, aacombine], ignore_index=True)
        sortTable = CombineTable_df.copy()
        # Convert Cost Center to 5-digit strings
        sortTable['Cost Center'] = sortTable['Cost Center'].astype(str).str.zfill(5)
    
        # Convert Transaction Date to datetime for proper sorting
        sortTable['Transaction Date'] = pd.to_datetime(
            sortTable['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )
    
        # Sort by Cost Center then Transaction Date
        sortTable = sortTable.sort_values(by=['Cost Center', 'Transaction Date'])
    
        # Convert Transaction Date back to original format
        sortTable['Transaction Date'] = sortTable['Transaction Date'].dt.strftime('%d/%m/%Y')
        self.Combined_df = sortTable
        
        return self.Combined_df
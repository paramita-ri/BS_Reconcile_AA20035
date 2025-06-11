import pandas as pd

class AAProcess():
    
    def __init__(self, AACurrent, GTO05, Period_Date):
        self.AACurrent_df  = AACurrent
        self.GTO05_df = GTO05
        self.Period_Date = Period_Date
        self.AAPivot_df = None
        self.AAForCombine_df = None
  
    #make pivot by Amount from AA       
    def getAApivot(self):
        pivot_df = self.AACurrent_df.copy()
        pivot_df = pd.pivot_table(
            pivot_df,
            values='Amount',
            index=['Cost Center', 'Transaction Date'],
            aggfunc='sum'
        ).reset_index()
        
        pivot_df['Amount'] = pivot_df['Amount'].round(2)
        pivot_df = pivot_df[
            (pivot_df['Amount'] != 0) & 
            (~pivot_df['Amount'].isna())
        ]
        self.AAPivot_df = pivot_df
        self.FillColumn()
        self.AAPivot_df = self.AAPivot_df.reset_index(drop=True)
        self.AAForCombine_df = self.AAForCombine_df.reset_index(drop=True)

        return self.AAForCombine_df
    
    #fill column after pivot from for combine with reconcile report
    def FillColumn(self):
    
        fillColumn_df = self.AAPivot_df.copy()
        aacurrent_df = self.AACurrent_df.copy()
        
        fillColumn_df['Period'] = self.Period_Date

        fillColumn_df['LOB'] = "00023"

        building_mapping = self.GTO05_df.set_index('Cost Center Number')['Building Name'].to_dict()

        fillColumn_df['Building'] = fillColumn_df['Cost Center'].map(building_mapping)

        fillColumn_df['Group'] = None
        fillColumn_df['Status/Reported GTO No.'] = None
        fillColumn_df['Remark'] = None
        fillColumn_df['Question'] = None
        fillColumn_df['Vendor'] = None
        
        fillColumn_df = fillColumn_df.rename(columns={'Amount': 'Total'})
        column_order = [
            'Period', 'Cost Center', 'Building', 'LOB', 'Transaction Date',
            'Total', 'Group', 'Vendor', 'Status/Reported GTO No.','Remark','Question'
        ]
        fillColumn_df = fillColumn_df[column_order]
        
        self.AAForCombine_df = fillColumn_df
        
           
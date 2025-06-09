import pandas as pd
import calendar
from datetime import datetime

class PendingBills():
    def __init__(self, Combined_df, Period_Date):
        self.Combined_df = Combined_df
        self.Period_Date = Period_Date
        self.LastDay = None
        self.PendingBills_df = None
        self.ForBalance_df = None
        
    #Get the last day of the month for any given period date (DD/MM/YYYY format)    
    def getLastDay(self):
        
        period_dt = pd.to_datetime(self.Period_Date, format='%d/%m/%Y')
        last_day = calendar.monthrange(period_dt.year, period_dt.month)[1]
        self.LastDay = period_dt.replace(day=last_day)
        return self.LastDay
    
    def Separeate_table(self):
        
        combined_df = self.Combined_df.copy()
        # Convert to datetime and find period's last day
        combined_df['Transaction Date'] = pd.to_datetime(
            combined_df['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )
        lastDay = self.getLastDay()
        
        # Identify pending bills (last day transactions)
        pending_mask = (combined_df['Transaction Date'] == lastDay) & (combined_df['Total'] < 0)
        self.PendingBills_df = combined_df[pending_mask].copy()
        self.ForBalance_df = combined_df[~pending_mask].copy()
        
        # Convert dates back to original format
        for df in [self.PendingBills_df, self.ForBalance_df]:
            df['Transaction Date'] = df['Transaction Date'].dt.strftime('%d/%m/%Y')
    
        

    def getPendingBills(self):
        
        self.Separeate_table()
        pendingBill = self.PendingBills_df.copy()
        period_date = self.Period_Date
        period_date = datetime.strptime(period_date, "%d/%m/%Y")
        formatted_period = period_date.strftime("%b-%Y")
        message = f'ยอดขาย Food Court ({formatted_period}) ที่ลูกค้ายังไม่มาวางบิล'
        pendingBill['Group'] = message
        self.PendingBills_df = pendingBill
        
        return self.PendingBills_df, self.ForBalance_df
import pandas as pd
import numpy as np
from datetime import datetime
from itertools import combinations

class Minimum():
    def __init__(self,Balance_df, AAstart_df, GTO_df):
            self.Balanced_df = Balance_df
            self.AAstar_df = AAstart_df
            self.GTO05_df = GTO_df
            self.MergeID = None
            self.MMG_df = None
            self.Refund_df = None
            self.Minimum_df = None
            self.MMG_Refund = None
            
    def getMMG_df(self):
        
        balanced = self.Balanced_df.copy()
        aastar = self.AAstar_df.copy()
        gto05 = self.GTO05_df.copy()
        #simplifie column name in aastar and gto05
        aastar = aastar.rename(columns={
            'MRI or Anacle TransactionID':'TS_ID',
            'Period Name':'Period'
        })
        gto05 = gto05.rename(columns={
            'Reported GTO No.':'TS_ID',
            'Customer':'Vendor',
            'Adjusted MMG GP Amount':'MMG'
        })
        aastar['Period'] = pd.to_datetime(
            aastar['Period'],
            format='%b-%y', errors='coerce'
        )
        balanced['Period'] = pd.to_datetime(
            balanced['Period'],
            format='%d/%m/%Y', errors='coerce'
        )
        #drop row that has nan in critical column and left only column 
        #we want in aastar and gto05
        aastar = aastar.dropna(subset=['TS_ID'])[['Period','Cost Center', 'Transaction Date', 'TS_ID']]
        gto05 = gto05.query("MMG.notna() and MMG != 0")[['TS_ID', 'Vendor', 'MMG']]
        balanced = balanced[['Period', 'Cost Center', 'Transaction Date']]
        
        mergeID_df = pd.merge(balanced, aastar, on=['Period','Cost Center', 'Transaction Date'], how='left')
        mergeID_df = mergeID_df.dropna(subset=['TS_ID'])
        
        mergeMMG_df = pd.merge(mergeID_df, gto05, on=['TS_ID'], how='left')
        mergeMMG_df = mergeMMG_df.dropna(subset=['MMG', 'Vendor'])
        mergeMMG_df['Period'] = mergeMMG_df['Period'].dt.strftime('%d/%m/%Y')
        mergeID_df['Period'] = mergeID_df['Period'].dt.strftime('%d/%m/%Y')
        self.MergeID = mergeID_df
        self.MMG_df = mergeMMG_df
        return self.MMG_df 
    
    def getRefund_df(self):
        
        balanced = self.Balanced_df.copy()
        aastar = self.AAstar_df.copy()
        gto05 = self.GTO05_df.copy()
        #simplifie column name in aastar and gto05
        aastar = aastar.rename(columns={
            'MRI or Anacle TransactionID':'TS_ID',
            'Period Name':'Period'
        })
        gto05 = gto05.rename(columns={
            'Reported GTO No.':'TS_ID',
            'Customer':'Vendor',
            'COGS Refund':'Refund'
        })
        aastar['Period'] = pd.to_datetime(
            aastar['Period'],
            format='%b-%y', errors='coerce'
        )
        balanced['Period'] = pd.to_datetime(
            balanced['Period'],
            format='%d/%m/%Y', errors='coerce'
        )
        #drop row that has nan in critical column and left only column 
        #we want in aastar and gto05
        aastar = aastar.dropna(subset=['TS_ID'])[['Period','Cost Center', 'Transaction Date', 'TS_ID']]
        gto05 = gto05.query("Refund.notna() and Refund != 0")[['TS_ID', 'Vendor', 'Refund']]
        balanced = balanced[['Period', 'Cost Center', 'Transaction Date']]
        
        mergeID_df = pd.merge(balanced, aastar, on=['Period', 'Cost Center', 'Transaction Date'], how='left')
        mergeID_df = mergeID_df.dropna(subset=['TS_ID'])
        
        mergeRefund_df = pd.merge(mergeID_df, gto05, on=['TS_ID'], how='left')
        mergeRefund_df = mergeRefund_df.dropna(subset=['Refund', 'Vendor'])
        mergeRefund_df['Period'] = mergeRefund_df['Period'].dt.strftime('%d/%m/%Y')
        self.Refund_df = mergeRefund_df
        return self.Refund_df 
          
    def getMinimum_df(self):
        mergeMMG = self.getMMG_df()
        MMG_df = mergeMMG.copy()
        minimum_df = self.Balanced_df.copy()
        final_rows = []

        if 'Total' not in minimum_df.columns:
            return minimum_df

        MMG_df['MMG'] = MMG_df['MMG'].round(2)
        minimum_df['Cost Center'] = minimum_df['Cost Center'].astype(str).apply(lambda x: x.zfill(5))
        MMG_df['Cost Center'] = MMG_df['Cost Center'].astype(str).apply(lambda x: x.zfill(5))
        minimum_df['LOB'] = minimum_df['LOB'].astype(str).apply(lambda x: x.zfill(5))

        for cost_center, group_df in minimum_df.groupby('Cost Center'):
            group_df = group_df.reset_index(drop=True)
            mmg_rows = MMG_df[MMG_df['Cost Center'] == cost_center].copy()

            if mmg_rows.empty:
                final_rows.extend(group_df.to_dict(orient='records'))
                continue

            group_df['Transaction Date'] = pd.to_datetime(group_df['Transaction Date'], format='%d/%m/%Y', errors='coerce')
            mmg_rows['Transaction Date'] = pd.to_datetime(mmg_rows['Transaction Date'], format='%d/%m/%Y', errors='coerce')
            n = len(group_df)
            used_indexes = set()

            # Step 1: One-to-one direct matching
            for i in range(n):
                if i in used_indexes:
                    continue

                row = group_df.loc[i]
                total = round(row['Total'], 2)
                match = mmg_rows[(mmg_rows['MMG'] == -total) & (mmg_rows['Transaction Date'] == row['Transaction Date'])]

                if not match.empty:
                    matched_row = match.iloc[0]
                    new_row = row.copy()
                    new_row['Total'] = total
                    new_row['Period'] = matched_row['Period']
                    new_row['Vendor'] = matched_row['Vendor']
                    new_row['Status/Reported GTO No.'] = matched_row['TS_ID']
                    year = pd.to_datetime(matched_row['Period'], dayfirst=True).year
                    new_row['Group'] = f"Minimum Guarantee Y{year}"
                    final_rows.append(new_row.to_dict())
                    used_indexes.add(i)

            # Step 2: Consecutive group matching from largest to smallest
            for size in reversed(range(2, n + 1)):
                for start in range(n - size + 1):
                    idx_range = list(range(start, start + size))
                    if any(i in used_indexes for i in idx_range):
                        continue

                    subset = group_df.loc[idx_range]
                    total_sum = subset['Total'].sum()
                    total_sum_rounded = round(total_sum, 2)
                    match = mmg_rows[mmg_rows['MMG'] == -total_sum_rounded]

                    if not match.empty:
                        matched_row = match.iloc[0]
                        match_date = pd.to_datetime(matched_row['Transaction Date'])
                        matched_sub = subset[subset['Transaction Date'] == match_date]

                        if matched_sub.empty:
                            continue

                        base_row = matched_sub.iloc[0].copy()
                        new_row = base_row.copy()
                        new_row['Total'] = total_sum_rounded
                        new_row['Period'] = matched_row['Period']
                        new_row['Vendor'] = matched_row['Vendor']
                        new_row['Status/Reported GTO No.'] = matched_row['TS_ID']
                        year = pd.to_datetime(matched_row['Period'], dayfirst=True).year
                        new_row['Group'] = f"Minimum Guarantee Y{year}"
                        final_rows.append(new_row.to_dict())
                        used_indexes.update(idx_range)

            # Step 3: Add unmatched rows
            for i in range(n):
                if i not in used_indexes:
                    final_rows.append(group_df.loc[i].to_dict())

        result_df = pd.DataFrame(final_rows)
        result_df['Transaction Date'] = pd.to_datetime(result_df['Transaction Date'], errors='coerce')
        result_df = result_df.sort_values(by=['Cost Center', 'Transaction Date'])
        result_df['Transaction Date'] = result_df['Transaction Date'].dt.strftime('%d/%m/%Y')
        result_df['Group'] = result_df['Group'].fillna("Pending for user manual")        
        # Create a dictionary from MMG_df with tuple keys
        key_cols = ['Cost Center', 'Period', 'Transaction Date']
        mapping_dict = self.MergeID.set_index(key_cols)['TS_ID'].to_dict()

        # Create a function to apply the mapping
        def fill_status(row):
            if pd.isna(row['Status/Reported GTO No.']):
                key = (row['Cost Center'], row['Period'], row['Transaction Date'])
                return mapping_dict.get(key, np.nan)
            return row['Status/Reported GTO No.']

        result_df['Status/Reported GTO No.'] = result_df.apply(fill_status, axis=1)

        self.Minimum_df = result_df
        return self.Minimum_df

    def getMMG_Refund(self):
        mmg = self.getMMG_df().copy()
        refund = self.getRefund_df().copy()
        minimum = self.Minimum_df.copy()

        # Ensure Period is datetime for all dfs (if not already done)
        for df in [mmg, refund, minimum]:
            if 'Period' in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df['Period']):
                    df['Period'] = pd.to_datetime(df['Period'], errors='coerce')

        # Merge on the 3 keys
        combined_df = pd.merge(
            mmg, refund,
            on=['Cost Center', 'Transaction Date', 'Period'],
            how='outer',
            suffixes=('_MMG', '_Refund')
        )

        # Combine Vendor and TS_ID columns safely
        combined_df['Vendor'] = combined_df['Vendor_MMG'].combine_first(combined_df['Vendor_Refund'])
        combined_df['TS_ID'] = combined_df['TS_ID_MMG'].combine_first(combined_df['TS_ID_Refund'])

        # Select desired columns
        combined_df = combined_df[['Period', 'Cost Center', 'Transaction Date', 'TS_ID', 'Vendor', 'MMG', 'Refund']]

        # Check if 'Group' column exists in minimum df, if not create an empty mask
        if 'Group' in minimum.columns:
            mask = minimum['Group'].str.contains("Minimum Guarantee", case=False, na=False)
        else:
            mask = pd.Series([False] * len(minimum))

        nominimum = minimum[~mask].copy()

        # Ensure Cost Center types match, cast both to string (safe approach)
        combined_df['Cost Center'] = combined_df['Cost Center'].astype(str)
        nominimum['Cost Center'] = nominimum['Cost Center'].astype(str)

        # Filter combined_df Cost Centers by nominimum Cost Centers
        valid_cost_centers = set(nominimum['Cost Center'].unique())
        filtered_df = combined_df[combined_df['Cost Center'].isin(valid_cost_centers)].reset_index(drop=True)

        self.MMG_Refund = filtered_df
        return self.MMG_Refund
    

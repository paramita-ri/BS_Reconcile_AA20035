import pandas as pd
import numpy as np
from datetime import datetime
from GetReport import *
from itertools import combinations

class Minimum2():
    def __init__(self):
        self.Balanced_df = pd.read_excel("/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/Output/Balanced_df.xlsx")
        self.AAstar_df = pd.read_excel("/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/Output/AAStar_df.xlsx")
        self.GTO05_df = pd.read_excel("/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/Output/GTO05_df.xlsx")
        self.toFindminimum_df = None
        self.Minimum_df = None

    def getToFindminimum_df(self):
        balanced = self.Balanced_df.copy()
        aastar = self.AAstar_df.copy()
        gto05 = self.GTO05_df.copy()

        aastar = aastar.rename(columns={
            'MRI or Anacle TransactionID': 'TS_ID'
        })
        gto05 = gto05.rename(columns={
            'Reported GTO No.': 'TS_ID',
            'Customer': 'Vendor',
            'Adjusted MMG GP Amount': 'MMG'
        })

        aastar = aastar.dropna(subset=['TS_ID'])[['Cost Center', 'Transaction Date', 'TS_ID']]
        gto05 = gto05.query("MMG.notna() and MMG != 0")[['TS_ID', 'Vendor', 'MMG']]
        balanced = balanced[['Period', 'Cost Center', 'Transaction Date', 'Total']]

        mergeID_df = pd.merge(balanced, aastar, on=['Cost Center', 'Transaction Date'], how='left')
        mergeID_df = mergeID_df.dropna(subset=['TS_ID'])

        mergeMMG_df = pd.merge(mergeID_df, gto05, on=['TS_ID'], how='left')
        mergeMMG_df = mergeMMG_df.dropna(subset=['MMG'])

        return mergeMMG_df

    def getMinimum_df(self):
        self.toFindminimum_df = self.getToFindminimum_df()
        MMG_df = self.toFindminimum_df.copy()
        minimum_df = self.Balanced_df.copy()
        final_rows = []

        if 'Total' not in minimum_df.columns:
            print("❌ ERROR: 'Total' column not found in Balanced_df")
            return minimum_df

        print("✅ Starting Minimum Guarantee reconciliation...\n")

        MMG_df['MMG'] = MMG_df['MMG'].round(2)
        minimum_df['Cost Center'] = minimum_df['Cost Center'].astype(str).apply(lambda x: x.zfill(5))
        MMG_df['Cost Center'] = MMG_df['Cost Center'].astype(str).apply(lambda x: x.zfill(5))

        if 'LOB' in minimum_df.columns:
            minimum_df['LOB'] = minimum_df['LOB'].astype(str).apply(lambda x: x.zfill(5))

        for cost_center, group_df in minimum_df.groupby('Cost Center'):
            print(f"\n🔎 Checking Cost Center: {cost_center}")
            group_df = group_df.reset_index(drop=True)
            mmg_rows = MMG_df[MMG_df['Cost Center'] == cost_center].copy()

            if mmg_rows.empty:
                print("  ⚠️ No MMG entries found.")
                final_rows.extend(group_df.to_dict(orient='records'))
                continue

            group_df['Transaction Date'] = pd.to_datetime(group_df['Transaction Date'], format='%d/%m/%Y', errors='coerce')
            mmg_rows['Transaction Date'] = pd.to_datetime(mmg_rows['Transaction Date'], format='%d/%m/%Y', errors='coerce')
            n = len(group_df)
            used_indexes = set()

            # 1. One-to-one direct matching
            print("  🔹 Step 1: One-to-one direct matching...")
            for i in range(n):
                if i in used_indexes:
                    continue

                row = group_df.loc[i]
                total = round(row['Total'], 2)
                match = mmg_rows[(mmg_rows['MMG'] == -total) & (mmg_rows['Transaction Date'] == row['Transaction Date'])]

                print(f"    ➤ Row {i}: Total = {total}, Date = {row['Transaction Date'].date()}")
                if not match.empty:
                    matched_row = match.iloc[0]
                    print(f"      ✅ Match found with MMG = {matched_row['MMG']} on {matched_row['Transaction Date'].date()}")
                    new_row = row.copy()
                    new_row['Total'] = total
                    new_row['Period'] = matched_row['Period']
                    new_row['Vendor'] = matched_row['Vendor']
                    new_row['Status/Reported GTO No.'] = matched_row['TS_ID']
                    year = pd.to_datetime(matched_row['Period'], dayfirst=True).year
                    new_row['Group'] = f"Minimum Guarantee Y{year}"
                    final_rows.append(new_row.to_dict())
                    used_indexes.add(i)
                else:
                    print("      ❌ No match.")

            # 2. Consecutive group matching from largest to smallest
            print("  🔹 Step 2: Consecutive group matching from largest to smallest...")
            for size in reversed(range(2, n + 1)):
                for start in range(n - size + 1):
                    idx_range = list(range(start, start + size))
                    if any(i in used_indexes for i in idx_range):
                        continue

                    subset = group_df.loc[idx_range]
                    total_sum = subset['Total'].sum()
                    total_sum_rounded = round(total_sum, 2)
                    print(f"    ➤ Trying rows {start}-{start + size - 1}, Sum = {total_sum} -> Rounded = {total_sum_rounded}")

                    match = mmg_rows[mmg_rows['MMG'] == -total_sum_rounded]
                    if not match.empty:
                        matched_row = match.iloc[0]
                        match_date = pd.to_datetime(matched_row['Transaction Date'])
                        matched_sub = subset[subset['Transaction Date'] == match_date]

                        if matched_sub.empty:
                            print(f"      ⚠️ No matching date {match_date.date()} in subset.")
                            continue

                        print(f"      ✅ Match found: MMG = {matched_row['MMG']} at {match_date.date()}")
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
                    else:
                        print(f"      ❌ No MMG match for rounded sum {total_sum_rounded}")

            # Add unmatched rows
            for i in range(n):
                if i not in used_indexes:
                    final_rows.append(group_df.loc[i].to_dict())

        result_df = pd.DataFrame(final_rows)
        result_df['Transaction Date'] = pd.to_datetime(result_df['Transaction Date'], errors='coerce')
        result_df = result_df.sort_values(by=['Cost Center', 'Transaction Date'])
        result_df['Transaction Date'] = result_df['Transaction Date'].dt.strftime('%d/%m/%Y')
        print("\n✅ Reconciliation Complete.") 
        result_df['Group'] = result_df['Group'].fillna("Pending for user manual")
        self.Minimum_df = result_df

        return self.Minimum_df

    def septab(self, minimum_df):
        minimum_df['Cost Center'] = minimum_df['Cost Center'].astype(str).str.zfill(5)
        minimum_df['LOB'] = minimum_df['LOB'].astype(str).str.zfill(5)
        # Create a boolean mask where 'Group' contains "Minimum Guarantee" (case-insensitive)
        mask = minimum_df['Group'].str.contains("Minimum Guarantee", case=False, na=False)

        # Rows with "Minimum Guarantee" in 'Group'
        minimum_guarantee_df = minimum_df[mask].copy()

        # Rows without "Minimum Guarantee" in 'Group'
        userManual_df = minimum_df[~mask].copy()
        return minimum_guarantee_df, userManual_df
        
    
    def getRefund(self):#COGS Refund
        balanced = self.Balanced_df.copy()
        aastar = self.AAstar_df.copy()
        gto05 = self.GTO05_df.copy()

        aastar = aastar.rename(columns={
            'MRI or Anacle TransactionID': 'TS_ID'
        })
        gto05 = gto05.rename(columns={
            'Reported GTO No.': 'TS_ID',
            'Customer': 'Vendor',
            'COGS Refund': 'COGS'
        })

        aastar = aastar.dropna(subset=['TS_ID'])[['Cost Center', 'Transaction Date', 'TS_ID']]
        gto05 = gto05.query("COGS.notna() and COGS != 0")[['TS_ID', 'Vendor', 'COGS']]
        balanced = balanced[['Period', 'Cost Center', 'Transaction Date']]

        mergeID_df = pd.merge(balanced, aastar, on=['Cost Center', 'Transaction Date'], how='left')
        mergeID_df = mergeID_df.dropna(subset=['TS_ID'])

        mergeCOGS_df = pd.merge(mergeID_df, gto05, on=['TS_ID'], how='left')
        mergeCOGS_df = mergeCOGS_df.dropna(subset=['COGS', 'Vendor'])

        return mergeCOGS_df

if __name__ == "__main__":
    minimum = Minimum2()
    minimum_df = minimum.getMinimum_df()
    #mimimummy = pd.read_excel("/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/Output/test/minimum_df2_2.xlsx")
    #minimum_MMG_df, userMan_df = minimum.septab(mimimummy)
    #refund_df = minimum.getRefund()
    save = GetReport(minimum_df,minimum_df)
    #save.save_to_excel(minimum_df,"save minimum_df", "minimum_df.xlsx")
    #save.save_to_excel(minimum_MMG_df,"save minimum_MMG_df","minimum_MMG_df.xlsx")
    #save.save_to_excel(userMan_df,"save userMan_df","userMan_df.xlsx")
    save.save_to_excel(minimum_df,"save refund_df","refund_df.xlsx")

    
import numpy as np
import pandas as pd
import itertools
from numba import njit
from collections import Counter
from GetReport import *

class Balance2():
    def __init__(self):#, ForBalance_df
        self.ForBalance_df = pd.read_excel("/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/Output/ForBalance_df.xlsx")
        self.dropPair_df = None
        self.Balanced_df = None
        
        
    def getDropPairs(self):
        df = self.ForBalance_df.copy()
        df.reset_index(drop=True)
        matched_indices = set()

        # Group by Cost Center
        for cc, group in df.groupby('Cost Center'):
            group = group.copy()
            totals = group['Total'].tolist()
            indices = group.index.tolist()  # preserve actual indices

            counter = Counter(totals)
            handled = set()

            # Try to match zero-sum pairs
            for amt in list(counter.keys()):
                if amt in handled or -amt not in counter:
                    continue

                pos_count = counter[amt]
                neg_count = counter[-amt]
                pair_count = min(pos_count, neg_count)

                # Find actual row indices of matching pairs
                if pair_count > 0:
                    amt_indices = [i for i in indices if np.isclose(df.at[i, 'Total'], amt, atol=1e-2) and i not in matched_indices]
                    neg_indices = [i for i in indices if np.isclose(df.at[i, 'Total'], -amt, atol=1e-2) and i not in matched_indices]

                    for i in range(pair_count):
                        if i < len(amt_indices) and i < len(neg_indices):
                            matched_indices.add(amt_indices[i])
                            matched_indices.add(neg_indices[i])

                handled.add(amt)
                handled.add(-amt)

        # Drop all matched rows and return remaining rows
        self.dropPair_df = df.drop(index=matched_indices).reset_index(drop=True)

    
    def dropThreeUp(self):
        dropPair_df = self.dropPair_df.copy()
        keep_dfs = []

        for cc in dropPair_df['Cost Center'].unique():
            sub_df = dropPair_df[dropPair_df['Cost Center'] == cc].copy()
            used_indices = set()

            for r in range(3, min(len(sub_df), 6) + 1):
                for combo in itertools.combinations(sub_df.index, r):
                    if set(combo).intersection(used_indices):
                        continue
                    total = sub_df.loc[list(combo), 'Total'].sum()
                    if np.isclose(total, 0, atol=1e-2):
                        used_indices.update(combo)

            keep_df = sub_df[~sub_df.index.isin(used_indices)]
            if not keep_df.empty:
                keep_dfs.append(keep_df)

        self.Balanced_df = pd.concat(keep_dfs, ignore_index=True)


    
    def getBalance(self):
        self.dropPair()
        self.dropThreeUp()
        return self.Balanced_df

if __name__ == "__main__":
    balance = Balance2()
    dropPair = balance.dropPair()
    save = GetReport(dropPair,dropPair)
    save.save_to_excel(balance,"save drop pair", "droppair.xlsx")
    
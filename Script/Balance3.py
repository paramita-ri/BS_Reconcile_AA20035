import numpy as np
import pandas as pd
import itertools
from numba import njit
from collections import Counter
from GetReport import *

class Balance3():
    def __init__(self):#, ForBalance_df
        self.ForBalance_df = pd.read_excel("/Users/peposeemuang/Desktop/BS_Reconcile_AA20035/Output/ForBalance_df.xlsx")
        self.dropPair_df = None
        self.Balanced_df = None
        
        
    def dropPair(self):
        # Step 1: Handle 2-number zero-sum pairs
        forbalance_df = self.ForBalance_df.reset_index(drop=True).copy()
        keep_rows = []

        for cc in forbalance_df['Cost Center'].unique():
            sub_df = forbalance_df[forbalance_df['Cost Center'] == cc].copy()
            used_indices = set()
            index_list = sub_df.index.tolist()

            for i in range(len(index_list)):
                idx_i = index_list[i]
                if idx_i in used_indices:
                    continue

                for j in range(i + 1, len(index_list)):
                    idx_j = index_list[j]
                    if idx_j in used_indices:
                        continue

                    total = sub_df.at[idx_i, 'Total'] + sub_df.at[idx_j, 'Total']
                    if np.isclose(total, 0, atol=1e-2):
                        used_indices.update([idx_i, idx_j])
                        break

            # Keep only unused rows
            unused = sub_df[~sub_df.index.isin(used_indices)]
            if not unused.empty:
                keep_rows.append(unused)

        self.dropPair_df = pd.concat(keep_rows, ignore_index=True)

    @njit
    def find_zero_sum_groups(totals, min_group=3, max_group=6, atol=1e-2):
        n = len(totals)
        used = np.zeros(n, dtype=np.bool_)
        result_indices = []

        for r in range(min_group, min(max_group, n) + 1):
            for combo in itertools.combinations(range(n), r):
                if any(used[i] for i in combo):
                    continue
                s = 0.0
                for i in combo:
                    s += totals[i]
                if np.abs(s) < atol:
                    for i in combo:
                        used[i] = True
        return used

    def dropThreeUp(self):
        dropPair_df = self.dropPair_df.copy()
        keep_dfs = []

        for cc in dropPair_df['Cost Center'].unique():
            sub_df = dropPair_df[dropPair_df['Cost Center'] == cc].copy()
            used_indices = set()

            for r in range(3, min(len(sub_df), 7) + 1):
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
    balance = Balance3()
    balance_df = balance.getBalance()
    save = GetReport(balance.dropPair_df,balance.dropPair_df)
    save.save_to_excel(balance_df,"save drop pair", "droppair.xlsx")
    
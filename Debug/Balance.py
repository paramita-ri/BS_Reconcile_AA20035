import pandas as pd
import numpy as np
import itertools
   
class Balance():
    def __init__(self):
        self.ForBalance_df = pd.read_excel("/Users/peposeemuang/Desktop/Output/ForBalance_df.xlsx")
        self.dropPair_df = None
        self.dropAll_df = None  # New intermediate dataframe
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
                
                if i + 1 < len(index_list):
                    for j in range(i+1, i+3):
                        j = i + 1
                        idx_j = index_list[j]
                        total = sub_df.at[idx_i, 'Total'] + sub_df.at[idx_j, 'Total']
                        if np.isclose(total, 0, atol=1e-2):
                            used_indices.update([idx_i, idx_j])


            # Keep only unused rows
            unused = sub_df[~sub_df.index.isin(used_indices)]
            if not unused.empty:
                keep_rows.append(unused)

        self.dropPair_df = pd.concat(keep_rows, ignore_index=True)
    
    def dropAllBalanced(self):
        """Drop entire cost centers where all rows sum to zero"""
        dropPair_df = self.dropPair_df.copy()
        keep_dfs = []
        
        for cc in dropPair_df['Cost Center'].unique():
            sub_df = dropPair_df[dropPair_df['Cost Center'] == cc].copy()
            total = sub_df['Total'].sum()
            
            # Only keep cost centers that DON'T sum to zero
            if not np.isclose(total, 0, atol=1e-2):
                keep_dfs.append(sub_df)
        
        self.dropAll_df = pd.concat(keep_dfs, ignore_index=True)
    
    def dropThreeUp(self):
        # Change to use dropAll_df instead of dropPair_df
        dropAll_df = self.dropAll_df.copy()
        keep_dfs = []

        for cc in dropAll_df['Cost Center'].unique():
            sub_df = dropAll_df[dropAll_df['Cost Center'] == cc].copy()
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
        
    import pandas as pd

    def getBalance(self):
        self.dropPair()
        self.dropAllBalanced()  # Add the new step
        self.dropThreeUp()
        return self.Balanced_df
    
    

def save_dataframe_to_excel(dataframe, file_path, sheet_name='Sheet1', index=False):
    try:
        # Save DataFrame to Excel
        dataframe.to_excel(file_path, sheet_name=sheet_name, index=index)
        print(f"DataFrame successfully saved to {file_path}")
    except Exception as e:
        print(f"Error occurred while saving DataFrame to Excel: {e}")
    
if __name__ == "__main__":
    balancer = Balance()
    balanced_df = balancer.getBalance()
    save_dataframe_to_excel(balanced_df,"balance.xlsx")
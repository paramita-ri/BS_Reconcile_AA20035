import pandas as pd
from itertools import combinations

# Example: Load your reconcile sheet from a CSV (replace this with your real data loading)
# df = pd.read_csv('reconcile_sheet.csv')

# For testing: Sample data setup
data = {
    'Period': ['2025-05'] * 10,
    'Cost Center': [10012] * 10,
    'Building': ['A'] * 10,
    'LOB': ['Food'] * 10,
    'Transaction Date': pd.date_range(start='2025-05-01', periods=10, freq='D'),
    'Total': [10, 100, -100, -20, 20, 50, -15, -35, 70, 30],
    'Group': ['G1'] * 10,
    'Vendor': ['VendorA'] * 10,
    'Status/Reported GTO No.': ['OK'] * 10
}
df = pd.DataFrame(data)

def remove_simple_pairs(df):
    # Step 1: Handle 2-number zero-sum pairs
    result_df = pd.DataFrame(columns=df.columns)
    for cc in df['Cost Center'].unique():
        sub_df = df[df['Cost Center'] == cc].copy()
        sub_df['used'] = False  # mark matched rows
        for i in range(len(sub_df)):
            if sub_df.iloc[i]['used']:
                continue
            for j in range(i + 1, len(sub_df)):
                if sub_df.iloc[j]['used']:
                    continue
                if sub_df.iloc[i]['Total'] + sub_df.iloc[j]['Total'] == 0:
                    sub_df.at[sub_df.index[i], 'used'] = True
                    sub_df.at[sub_df.index[j], 'used'] = True
                    break
        # Append unused rows
        result_df = pd.concat([result_df, sub_df[sub_df['used'] == False].drop(columns='used')])
    return result_df.reset_index(drop=True)

def remove_complex_zero_sums(df):
    # Step 2: Handle multi-number zero-sum groups
    result_df = pd.DataFrame(columns=df.columns)
    for cc in df['Cost Center'].unique():
        sub_df = df[df['Cost Center'] == cc].copy()
        sub_df = sub_df.sort_values(by='Transaction Date').reset_index(drop=True)
        used_indices = set()

        i = 0
        while i < len(sub_df):
            if i in used_indices:
                i += 1
                continue
            found = False
            total_sum = sub_df.loc[i, 'Total']
            start = i
            temp_indices = [i]
            for j in range(i + 1, len(sub_df)):
                if j in used_indices:
                    continue
                total_sum += sub_df.loc[j, 'Total']
                temp_indices.append(j)

                if total_sum == 0:
                    used_indices.update(temp_indices)
                    found = True
                    break
                # Add skip rules:
                if sub_df.loc[start, 'Total'] > 0:
                    # Check pattern: -, + (fail)
                    if len(temp_indices) >= 3:
                        sign_seq = [sub_df.loc[idx, 'Total'] for idx in temp_indices[1:]]
                        if sign_seq[0] < 0 and sign_seq[1] > 0:
                            break
                elif sub_df.loc[start, 'Total'] < 0:
                    # Check pattern: +, - (fail)
                    if len(temp_indices) >= 3:
                        sign_seq = [sub_df.loc[idx, 'Total'] for idx in temp_indices[1:]]
                        if sign_seq[0] > 0 and sign_seq[1] < 0:
                            break
            if not found:
                i += 1
            else:
                i = max(temp_indices) + 1  # skip past used block

        # Keep only rows not used
        keep_df = sub_df.drop(index=used_indices)
        result_df = pd.concat([result_df, keep_df])
    return result_df.reset_index(drop=True)

# Run Step 1
df = remove_simple_pairs(df)
# Run Step 2
df = remove_complex_zero_sums(df)

# Final Result
print(df)


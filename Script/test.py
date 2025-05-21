import pandas as pd
from collections import Counter
import itertools

# Helper function to find the best zero-sum combination based on oldest date
def find_zero_sum_combinations(group):
    matched_indices = set()
    rows = group.to_dict('records')

    for r in range(2, len(rows) + 1):
        candidates = []

        for combo in itertools.combinations(rows, r):
            if sum(item['Amount'] for item in combo) == 0:
                indices = [item['index'] for item in combo]
                latest_date = max(item['Date'] for item in combo)

                if not any(idx in matched_indices for idx in indices):
                    candidates.append((indices, latest_date))

        if candidates:
            # Sort by latest date in group — we prefer older groups
            candidates.sort(key=lambda x: x[1])  # Oldest max(Date) first
            chosen_indices, _ = candidates[0]
            matched_indices.update(chosen_indices)

    return matched_indices


# Main reconciliation function
def reconcile(df):
    result_rows = []

    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])  # Make sure dates are datetime
    df['index'] = df.index  # Save original index

    # Loop through each CC group
    for cc, group in df.groupby('CC'):
        group = group.reset_index(drop=True)
        amounts = list(group['Amount'])
        counter = Counter(amounts)
        
        handled = set()

        for amt in list(counter.keys()):
            if amt in handled or -amt not in counter:
                continue

            pos_count = counter[amt]
            neg_count = counter[-amt]
            pair_count = min(pos_count, neg_count)

            if pos_count == neg_count:
                counter[amt] = 0
                counter[-amt] = 0
            else:
                counter[amt] -= pair_count
                counter[-amt] -= pair_count

                remain = abs(counter[amt] - counter[-amt])
                sign = 1 if counter[amt] > counter[-amt] else -1
                result_rows.extend([{'CC': cc, 'Amount': sign * abs(amt)}] * remain)

                counter[amt] = 0
                counter[-amt] = 0

            handled.add(amt)
            handled.add(-amt)

        # --- Step 3: Add any remaining unmatched amounts to results ---
        for amt, count in counter.items():
            if count > 0:
                result_rows.extend([{'CC': cc, 'Amount': amt}] * count)

    return pd.DataFrame(result_rows)

df = pd.DataFrame([
    {'CC': 20044, 'Amount': -100, 'Date': '2023-01-01'},
    {'CC': 20044, 'Amount': 20, 'Date': '2023-01-02'},
    {'CC': 20044, 'Amount': 50, 'Date': '2023-01-03'},
    {'CC': 20044, 'Amount': 30, 'Date': '2023-01-04'},
    {'CC': 20044, 'Amount': 80, 'Date': '2023-01-05'},
    {'CC': 20044, 'Amount': -60, 'Date': '2023-01-05'},
    {'CC': 20044, 'Amount': 60, 'Date': '2023-01-06'},
    {'CC': 20044, 'Amount': 40, 'Date': '2023-01-08'},
])

dff = reconcile(df)
print(dff)
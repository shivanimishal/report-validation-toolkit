"""
Report Validation Toolkit
Author: Shivani Mishal
Purpose: Compares a legacy report against a new automated output.
         Flags row count mismatches, schema changes, null shifts,
         and numeric variances — before they reach a stakeholder.
Business context: Comparing automated output against a
known-good baseline to detect failure modes systematically.
Usage: python validate.py --legacy old.csv --new new.csv --threshold 0.05
"""
 
# argparse: enables command-line arguments (--legacy, --new, --threshold)
import pandas as pd
import argparse
import sys
from datetime import datetime
 
# ── FILE LOADER ───────────────────────────────────────────────
# Loads a CSV file and prints a summary of what was loaded
# try/except: handles the case where the file path is wrong
# sys.exit(1): stops the script with an error code instead of crashing
def load_csv(path, label):
    try:
        df = pd.read_csv(path)    # Read CSV into a DataFrame (table)
        print(f'  Loaded {label}: {len(df):,} rows, {len(df.columns)} columns')
        return df
    except FileNotFoundError:
        print(f'ERROR: File not found — {path}')
        sys.exit(1)
 
# ── CHECK 1: ROW COUNT ────────────────────────────────────────
# Business purpose: If row counts change, it means records were added,
# dropped, or duplicated in the new pipeline — a critical data integrity signal
def check_row_counts(df_old, df_new):
    diff = len(df_new) - len(df_old)    # Positive = new report has more rows
    status = 'PASS' if diff == 0 else 'WARN'
    return {
        'check': 'Row Count',
        'status': status,
        'legacy': len(df_old),
        'new': len(df_new),
        'difference': diff,
        'detail': f'Row count changed by {diff:+,}'    # :+, shows + or - sign
    }
 
# ── CHECK 2: COLUMN SCHEMA ────────────────────────────────────
# Business purpose: Missing columns mean a report field was dropped from
# the new pipeline — a silent failure that breaks all downstream calculations
def check_columns(df_old, df_new):
    old_cols = set(df_old.columns)    # Convert column list to a set for comparison
    new_cols = set(df_new.columns)
    # Set subtraction: columns in old but not in new
    missing = old_cols - new_cols
    # Columns added in new that weren't in old
    added   = new_cols - old_cols
    # FAIL if any columns are missing (added columns are a WARN at most)
    status = 'PASS' if not missing else 'FAIL'
    return {
        'check': 'Column Schema',
        'status': status,
        'legacy': len(old_cols),
        'new': len(new_cols),
        'difference': len(missing) + len(added),
        'detail': f'Missing: {missing or None} | Added: {added or None}'
    }
 
# ── CHECK 3: NULL VALUE CHANGES ───────────────────────────────
# Business purpose: A spike in nulls in a key column (e.g. amount_usd)
# means the pipeline is failing to populate that field 
def check_nulls(df_old, df_new):
    results = []
    # Only check columns that exist in both reports
    common_cols = [c for c in df_old.columns if c in df_new.columns]
    for col in common_cols:
        old_nulls = df_old[col].isna().sum()    # Count nulls in legacy
        new_nulls = df_new[col].isna().sum()    # Count nulls in new
        # Only flag if the null count actually changed
        if old_nulls != new_nulls:
            results.append({
                'check': f'Null Count: {col}',
                'status': 'WARN',
                'legacy': old_nulls,
                'new': new_nulls,
                'difference': new_nulls - old_nulls,
                'detail': f'Null count changed in column {col}'
            })
    return results    # Returns a list — could be empty if no null changes
 
# ── CHECK 4: NUMERIC VARIANCE DETECTION ──────────────────────
# Business purpose: The most critical check — if total spend in the new
# report differs from legacy by more than the threshold (default 5%)
def check_numeric_variance(df_old, df_new, threshold):
    results = []
    common_cols = [c for c in df_old.columns if c in df_new.columns]
    # Filter to only numeric columns
    numeric_cols = [c for c in common_cols
                    if pd.api.types.is_numeric_dtype(df_old[c])
                    and pd.api.types.is_numeric_dtype(df_new[c])]
    for col in numeric_cols:
        old_sum = df_old[col].sum()    # Total of this column in legacy
        new_sum = df_new[col].sum()    # Total of this column in new
        if old_sum == 0:
            continue    # Skip if old total is zero — avoids divide-by-zero
        # Variance as a proportion: |new - old| / old
        variance = abs((new_sum - old_sum) / old_sum)
        status = 'PASS' if variance <= threshold else 'FAIL'
        if status == 'FAIL':    # Only log failures — keeps output clean
            results.append({
                'check': f'Variance: {col}',
                'status': status,
                'legacy': round(old_sum, 2),
                'new': round(new_sum, 2),
                'difference': round(variance * 100, 2),
                'detail': f'{variance*100:.1f}% variance exceeds {threshold*100:.0f}% threshold'
            })
    return results
 
# ── MAIN RUNNER ───────────────────────────────────────────────
# Orchestrates all checks and prints a formatted results table
def run_validation(legacy_path, new_path, threshold):
    print('\n=====================================================')
    print('  REPORT VALIDATION TOOLKIT')
    print(f'  Run date: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=====================================================\n')
    df_old = load_csv(legacy_path, 'Legacy Report')
    df_new = load_csv(new_path,    'New Report')
    print()
    # Run all checks and collect results into one list
    all_checks = []
    all_checks.append(check_row_counts(df_old, df_new))
    all_checks.append(check_columns(df_old, df_new))
    all_checks.extend(check_nulls(df_old, df_new))             # extend: adds a list
    all_checks.extend(check_numeric_variance(df_old, df_new, threshold))
    # Print results as a formatted table
    print(f"{'CHECK':<35} {'STATUS':<8} {'LEGACY':>12} {'NEW':>12}  DETAIL")
    print('-' * 88)
    for c in all_checks:
        print(f"{c['check']:<35} {c['status']:<8} {str(c['legacy']):>12} {str(c['new']):>12}  {c['detail']}")
    # Summary line
    fails = sum(1 for c in all_checks if c['status'] == 'FAIL')
    warns = sum(1 for c in all_checks if c['status'] == 'WARN')
    print(f'\nSUMMARY: {len(all_checks)} checks run | {fails} FAILED | {warns} WARNINGS')
    # Save results to CSV for audit trail
    pd.DataFrame(all_checks).to_csv('validation_results.csv', index=False)
    print('Results saved to: validation_results.csv\n')
 
# ── COMMAND LINE INTERFACE ────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Report Validation Toolkit')
    parser.add_argument('--legacy',    required=True)
    parser.add_argument('--new',       required=True)
    # default=0.05 means 5% threshold if user doesn't specify one
    parser.add_argument('--threshold', type=float, default=0.05)
    args = parser.parse_args()
    run_validation(args.legacy, args.new, args.threshold)

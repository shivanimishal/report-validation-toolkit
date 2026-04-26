# Report Validation Toolkit
 
## Business Problem
When migrating from a legacy reporting system to a new automated pipeline,
how do you systematically validate that outputs match? Manual checking is
error-prone and unscalable. This tool automates the comparison process.
 
## What It Does
- Row count comparison between legacy and new reports
- Column schema validation (missing or added columns flagged)
- Null value change detection per column
- Numeric variance detection — flags any column where values differ
  by more than the configured threshold (default: 5%)
- Outputs a structured CSV summary of all check results
 
## How to Run
```
pip install pandas
python validate.py --legacy legacy_report.csv --new new_report.csv --threshold 0.05
```
 
## Sample Output
```
CHECK                               STATUS   LEGACY     NEW    DETAIL
Row Count                           WARN         3        4    Row count changed by +1
Column Schema                       PASS         5        5    Missing: None | Added: None
Variance: total_spend               FAIL   525000   572000    8.9% variance (threshold: 5%)
```
 
## Business Context
Built to mirror the evaluation logic used when piloting AI-powered
automation tools — comparing AI-generated outputs against known-good
baselines to detect failure modes before production deployment.
 
## Tools
Python · pandas · argparse
 
## Author
Shivani Mishal

# Quality report — banking-engagement-warehouse

Generated: 2026-08-25T16:20:17.746964+00:00

**Overall score: 100%** (7/7 checks passed)

| Dimension | Score |
|---|---|
| completeness | 100% |
| correctness | 100% |
| validity | 100% |
| timeliness | 100% |

## Checks

| Dimension | Check | Measured | Threshold | Status | Detail |
|---|---|---|---|---|---|
| completeness | all_months_reconcile_bronze_eq_silver_plus_rejects | 3 | 3 | PASS | bronze_rows == silver_rows + rejects for every promoted month |
| correctness | clean_run_promotes_all_months | 3 | 3 | PASS | promoted ['month=00', 'month=01', 'month=02'] of 3 months on clean data |
| correctness | seeded_defect_blocks_exactly_the_bad_month | 1.0 | 1.0 | PASS | blocked=[{'month': 'month=02', 'failed_gates': ['range_outliers']}] |
| validity | blocked_month_has_no_silver_output | 0 | 0 | PASS | a month that fails gates must leave zero rows in silver, including revoking a prior successful run |
| validity | gold_reflects_only_promoted_months | 1.0 | 1.0 | PASS | gold has 109 rows, pipeline reported writing 109 |
| validity | gate_failure_publishes_sns_alert | 1.0 | 1.0 | PASS | SNS -> SQS alert for the blocked month reached the on-call queue |
| timeliness | clean_pipeline_run_under_sla | 36.5 | 180.0 | PASS | 3-month pipeline run wall time |

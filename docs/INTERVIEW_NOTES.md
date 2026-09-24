# Interview notes: Support Signals Lakehouse

## Resume bullets (update the numbers after your own full run with a GitHub token)

**AWS Customer Support Signals Lakehouse** | PySpark, SparkSQL, AWS Glue, S3, Redshift Serverless, Airflow, Terraform | [github link]

- Built an incremental ETL pipeline that ingests GitHub issues and comments from 4 AWS SDK/CLI/CDK repos into an S3 bronze/silver/gold lakehouse. It uses watermark-based extraction with a lookback window, handles rate limits, and delivers at least once; incremental runs process 628 new records in 6 s.
- Designed a Redshift star schema with an accumulating-snapshot fact table and an SCD Type 2 case dimension (DIST/SORT keys, staging-swap loads), enabling point-in-time backlog queries and time-to-first-response and SLA metrics by AWS service.
- Implemented 19 declarative data quality checks (uniqueness, referential integrity, freshness, SCD2 validity) that publish to CloudWatch and stop the Airflow DAG on failure. Caught real schema drift and a response-attribution bug that had left 98% of cases looking unanswered.
- Provisioned the platform with Terraform (S3, Glue, IAM least privilege, Redshift Serverless, CloudWatch alarms, SNS, a budget guardrail). CI with GitHub Actions runs unit, end-to-end and DAG-integrity tests plus `terraform validate`.

## Leadership Principle stories (STAR)

**Dive Deep: "98% of issues had no response"**
- *S:* On the first real run, a DQ warning showed almost no issues had a maintainer reply.
- *T:* Decide whether this was a data problem or a real finding.
- *A:* Profiled comment authors by `author_association` and found AWS staff mostly appear as `CONTRIBUTOR` because org membership is private. Made the responder definition configurable and documented why.
- *R:* Response coverage became meaningful. The remaining 62% with no response on recent issues is a real finding, and the check that caught the problem stays in place.

**Ownership: making retries safe**
- *S:* Airflow retries rerun a task with the same run ID.
- *A:* Found that the retry would overwrite the snapshot it was reading from. Changed publishes to write to unique directories with an atomic pointer flip, and added a test proving a retry changes nothing.
- *R:* Retries became idempotent, and rollback became a one-line pointer change.

**Insist on the Highest Standards: logic changes are backfills**
- A service-name normalization change produced 42 rows that the SCD2 merge correctly treated as "out of order". Instead of loosening the merge rule, I added a `--full-refresh` path so history is rebuilt consistently.

**Frugality**
- Designed to cost less than $20/month: Redshift Serverless at the 8-RPU minimum, 2 Glue workers, S3 Infrequent Access lifecycle, local Airflow instead of MWAA, and a budget alarm in Terraform.

## Likely deep-dive questions — have answers ready

- Why sort ascending on `updated_at`? What happens if two records share a timestamp across a page boundary? (The lookback window re-reads the overlap, and silver deduplicates it.)
- Why not Iceberg? (A deliberate MVP choice. The upgrade path is `MERGE INTO` on Glue 5, and the snapshot/pointer design mimics Iceberg's commit model.)
- How would you scale to 100× the data? (Partition silver by repo and month, rewrite incrementally with MERGE instead of the whole table, raise the number of Glue workers, and use Redshift incremental loads keyed on `_run_id`.)
- The SCD2 merge only sees states at run time. What do you lose? (Changes that happen between runs. The fix is ingesting the issue timeline/events API.)
- How do you know the dashboard numbers are right? (DQ checks, reconciliation of fact vs. silver row counts, and a unit test for each rule.)

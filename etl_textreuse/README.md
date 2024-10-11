# Spark ETL with Dagster Asset Management

The module which contains all the code for the Extract Transform Load (ETL) pipeline for the text reuse data using the Spark cluster managed by Dagster.

The Dagster maintains a Directed Acyclic Graph (DAG) of all the assets in the pipeline. These assets can be materialized from the Dagster interface.

## Organization

All the assets are found in the following code locations:

1. [`assets`](./assets/): All assets materialized in S3 buckets
2. [`database_assets](./database_assets/): All assets that will be materialized in MariaDB

There are two main S3 buckets that will be used throughout the setup, `raw_bucket` and `processed_bucket`. These can be the same S3 bucket or different ones. Their names need to be specified in the `.env` file like so:

```bash
RAW_BUCKET=<ALLAS BUCKET NAME>
PROCESSED_BUCKET=<ALLAS PROCESSED BUCKET NAME>
```


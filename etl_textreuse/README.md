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


## Upstream Dependencies

The `RAW_BUCKET` should contain the following upstream metadata sources.

1. `BLAST_ZIP_FILE` : The zip file containing JSONL files with raw BLAST hits. See [assets README](./assets/README.md)
   - `tr_data_out_all.zip` currently has all the BLAST hits from ECCO, EEBO-TCP and BL-Newspapers
   - `blast_reuses_test_100.zip` is a subset of the entire data with 100 JSONL files for testing purposes  
2. ECCO Metadata Assets: All the Parquet from COMHIS: [https://github.com/COMHIS/eccor/tree/main/inst/extdata](https://github.com/COMHIS/eccor/tree/main/inst/extdata) 
    - The `ecco_core` is the main metadata source
3. EEBO Metadata Assets: All the Parquet files from COMHIS: [https://github.com/COMHIS/eebor/tree/main/inst/extdata](https://github.com/COMHIS/eebor/tree/main/inst/extdata)
    - The `eebo_core` is the main metadata source
4. ESTC Metadata Assets: All the Parquet files from COMHIS: [https://github.com/COMHIS/estcr/tree/main/inst/estc_data](https://github.com/COMHIS/estcr/tree/main/inst/estc_data) 
    - `estc_core`, `estc_actors` and `estc_actor_links` are the main metadata sources used currently in the pipeline
5. NL Newspaper Metadata Assets: The `bl_newspapers_meta.csv` from Ville
   - The CSV is processed into a Parquet. See the [newspaper_core](./assets/upstream_metadata.py#L17) asset for more details.

These assets are define in Dagster as `AssetSpec`s and used to indicate the dependency. However, the file names are hardcoded when they need to be loaded from the `RAW_BUCKET`. The `AssetKey` for these upstream assets is meant to reflect the name of the underlying file. 

# Spark ETL with Dagster Asset Management

The module which contains all the code for the Extract Transform Load (ETL) pipeline for the text reuse data using the Spark cluster managed by Dagster.

The Dagster maintains a Directed Acyclic Graph (DAG) of all the assets in the pipeline. These assets can be materialized from the Dagster interface.

## Organization

All the assets are found in the following code locations:

1. [`assets`](./assets/): All assets materialized in S3 buckets
2. [`database_assets`](./database_assets/): All assets that will be materialized in MariaDB

There are two main S3 buckets that will be used throughout the setup, `raw_bucket` and `processed_bucket`. These can be the same S3 bucket or different ones. Their names need to be specified in the `.env` file like so:

```bash
RAW_BUCKET=<ALLAS BUCKET NAME>
PROCESSED_BUCKET=<ALLAS PROCESSED BUCKET NAME>
```


## Upstream Assets

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
   - The file is available from the project IDA at `/text_reuse/metadata/bl_newspapers.csv`
6. `RAW_TEXTS_ZIP_FILE`: The zip file containing all the raw text for each document. 
    - Default is the `textreuse_sources.zip` file on Allas
    - Created by unzipping all the files from `/scratch/project_2005072/ville/chunks_for_blast` in Puhti and making a single zip with jsonl files called `textreuse_sources.zip`
    - Moved to Allas in the `textreuse-raw-data` bucket
    - The entire file is 34GB and contains 3,071,516 entries.
    - File now available from the project IDA at `/text_reuse/metadata/textreuse_sources.zip` location

These assets are defined in Dagster as `AssetSpec`s and used to indicate the dependency. However, the file names are hard-coded when they need to be loaded from the `RAW_BUCKET`. The `AssetKey` for these upstream assets is meant to reflect the name of the underlying file. 

## Downstream Assets  

Using the upstream assets we extract, transform and load to create several downstream assets. These assets are categorized by following `group_names` in Dagster and the files that contain the assets related to each group:

1. `textreuses` : Assets related to the ETL of raw BLAST text reuses.
2. `metadata`: Assets related to the extraction and parsing of upstream metadata sources.
3. `downstream_metadata` : Gathering of metadata attributes useful for downstream tasks.
4. `downstream_textreuses` : Textreuse-based tables useful for downstream tasks.
5. `denormalized`: Denormalized versions of the `downstream_textreuses` assets meant specifically for loading into MariaDB for indexing.
6. `database`: Assets corresponding to the loading of Parquet dataframes into MariaDB database tables.

Each group of assets is described more in detail in the [assets/README.md](./assets/README.md) and [database_assets/README.md](./database_assets/README.md) files.

## Materializing Assets

From the Dagster interface, any asset can be materialized whenever required. Dagster will launch a run which in turn will start a Spark Application and materialize the resulting asset and store any available asset metadata to display.

Several assets can be materialized at once, for example, by selecting a `group_name` and materializing all assets with the same group name.

Similarly, if there is update to the logic of a particular asset, then the [code version](https://docs.dagster.io/concepts/assets/software-defined-assets#asset-code-versions) of that asset can be updated in the repository. For example, after updating the logic for the `manifestation_publication_dates` asset in [`publication_dates`](./assets/publication_date.py), we can update the code version as follows:

```python
@asset(
    deps=[ecco_core, eebo_core, newspapers_core,manifestation_ids,estc_core,"edition_ids","edition_mapping"],
    description="The publication year of each manifestation",
    group_name="downstream_metadata",
    code_version="2"
)
def manifestation_publication_date() -> Output[None]:
```

Then in the Dagster UI, the asset will indicate that the code has changes and also allow for updating all affected downstream assets in one easy go. The `code_version` also helps track the changes in logic as development proceeds.

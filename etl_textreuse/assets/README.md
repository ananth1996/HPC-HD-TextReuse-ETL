We describe the Dagster Assets from different groups. 

- [`textreuse` Assets](#textreuse-assets)
  - [Extracting Raw BLAST text reuses from zip file](#extracting-raw-blast-text-reuses-from-zip-file)
  - [Creating INT ids for each unique document](#creating-int-ids-for-each-unique-document)
  - [Dependency Diagram](#dependency-diagram)

# `textreuse` Assets

In this section, we describe all the assets from `textreuse` group. These assets are related to the ETL of the raw BLAST data.

1. Upstream Assets:
   - `BLAST_ZIP_FILE`: Name of zip file with Raw BLAST Text Reuses
2. S3 Materialized Assets
    - [`raw_textreuses`](/etl_textreuse/assets/raw_textreuses.py#L80)
    - [`textreuse_ids`](/etl_textreuse/assets/raw_textreuses.py#L141)
    - [`textreuses`](/etl_textreuse/assets/raw_textreuses.py#L181)
  
## Extracting Raw BLAST text reuses from zip file

Indicate the location of the S3 bucket and the name of zip file with the raw BLAST text reuses in the `.env` file:

```bash
RAW_BUCKET=<ALLAS BUCKET NAME>
BLAST_ZIP_FILE=<NAME OF BLAST ZIP FILE>
```

The structure of the zipfile should look like:  
```
tr_data_out_all.zip
|
|-tr_output_267.jsonl
|-tr_output_268.jsonl
|
```

Where each JSONL file has lines of raw text reuses from BLAST and each line looks like the following :

```json
{"text1_id": "0287901000", "text1_text_start": 87858, "text1_text_end": 87966, "text2_id": "0416900101", "text2_text_start": 3535059, "text2_text_end": 3535175, "align_length": 89, "positives_percent": 91.01}
```

The [`raw_textreuses`](/etl_textreuse/assets/raw_textreuses.py#L80) asset streams the ZIP file from the S3 bucket using Boto3, strams the JSON lines into a Parquet file with the following schema:

```bash
root
 |-- align_length: integer (nullable = true)
 |-- positives_percent: float (nullable = true)
 |-- text1_id: string (nullable = true)
 |-- text1_text: string (nullable = true)
 |-- text1_text_end: integer (nullable = true)
 |-- text1_text_start: integer (nullable = true)
 |-- text2_id: string (nullable = true)
 |-- text2_text: string (nullable = true)
 |-- text2_text_end: integer (nullable = true)
 |-- text2_text_start: integer (nullable = true)
```

Once the `raw_textreuses` has been materialized, we begin by creating integer ids for each unique source of text reuse by looking at the `text1_id` and `text2_id` attributes and then normalizing the raw hits into text piece and pairs of  reuses.

## Creating INT ids for each unique document

Once

## Dependency Diagram
```mermaid
graph TD;

BLAST_ZIP_FILE --> raw_textreuses;
raw_textreuses --> textreuse_ids;
raw_textreuses --> textreuses;
textreuse_ids --> textreuses;
textreuses --> orig_pieces;
orig_pieces --> orig_textreuses;
textreuses --> orig_textreuses;
orig_pieces --> piece_id_mappings;
piece_id_mappings --> defrag_textreuses;
orig_textreuses --> defrag_textreuses;
orig_pieces --> defrag_pieces;
piece_id_mappings --> defrag_pieces;
defrag_textreuses --> adjacency_list;
adjacency_list --> clusters;
clusters --> clustered_defrag_pieces;
```` 

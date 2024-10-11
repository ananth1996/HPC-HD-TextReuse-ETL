# Dagster Assets


## Texreuse Assets 

In this section, we describe all the assets from related to the text reuse data.

1. Upstream Assets:
   - Raw BLAST Text Reuses Zip file 
2. S3 Materialized Assets
    - [raw_textreuses](/etl_textreuse/assets/raw_textreuses.py#L80)

## Extract Raw BLAST TextReuses

Indicate the location of the S3 bucket and the name of zip file with the raw BLAST text reuses in the `.env` file:

```bash
RAW_BUCKET=<ALLAS BUCKET NAME>
BLAST_ZIP_FILE=<NAME OF BLAST ZIP FILE>
```

The structure of the zipfile should be like this 
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



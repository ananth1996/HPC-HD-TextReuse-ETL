#%%
from pathlib import Path
from time import perf_counter as time
from pyspark.sql.functions import *
from spark_utils import *
project_root = Path.cwd().resolve()
#%%
textreuses_raw = get_s3(fname="txtreuse",bucket=raw_bucket,table_name="textreuses_raw")
#%%
# get textreuse ids
textreuse_ids = materialise_row_numbers(
    fname = "textreuse_ids",
    df = spark.sql("""   
        SELECT 
            text1_id AS text_name,
            SUBSTRING_INDEX(text1_id,".",1) AS doc_name,
            SUBSTRING_INDEX(text1_id,".",-1) AS struct_name
        FROM textreuses_raw
        UNION
        SELECT 
            text2_id AS text_name, 
            SUBSTRING_INDEX(text2_id,".",1) AS doc_name,
            SUBSTRING_INDEX(text2_id,".",-1) AS struct_name 
        FROM textreuses_raw
        """),
    col_name="textreuse_source_id",
    bucket = args.output_s3_bucket)
#%%
textreuses = materialise_row_numbers(
    fname="textreuses",
    df=spark.sql("""
        SELECT 
            ti1.textreuse_source_id AS trs1_id,
            text1_text_start AS trs1_start,
            text1_text_end AS trs1_end,
            ti2.textreuse_source_id AS trs2_id,
            text2_text_start AS trs2_start,
            text2_text_end AS trs2_end,
            align_length,
            positives_percent
        FROM textreuses_raw t
        LEFT JOIN textreuse_ids ti1 ON t.text1_id = ti1.text_name
        LEFT JOIN textreuse_ids ti2 ON t.text2_id = ti2.text_name"""),
    col_name="textreuse_id",
    bucket=args.output_s3_bucket
)
#%%
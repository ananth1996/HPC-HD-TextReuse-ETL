#%%
from pathlib import Path
from time import perf_counter as time
from pyspark.sql.functions import *
from spark_utils import *
project_root = Path.cwd().resolve()
#%%
textreuses_raw = get_s3(fname="tr_data_out_all",bucket=raw_bucket,table_name="textreuses_raw")
#%%
# get textreuse ids
textreuse_ids = materialise_row_numbers(
    fname = "textreuse_ids",
    df = spark.sql("""   
    SELECT * FROM (
        SELECT 
            text1_id AS text_name,
            SUBSTRING_INDEX(text1_id ,".",1) as manifestation_id,
            (CASE 
		        WHEN LOCATE(".",text1_id) > 0 
                THEN SUBSTRING_INDEX(text1_id ,".",-1)
		        ELSE NULL
	        END) AS structure_name
        FROM textreuses_raw
        UNION
        SELECT 
            text2_id AS text_name, 
            SUBSTRING_INDEX(text2_id,".",1) AS manifestation_id,
            (CASE 
		        WHEN LOCATE(".",text2_id) > 0 
                THEN SUBSTRING_INDEX(text2_id ,".",-1)
		        ELSE NULL
	        END) AS structure_name
        FROM textreuses_raw
    ) 
    ORDER BY manifestation_id,structure_name"""),
    col_name="trs_id",
    bucket = processed_bucket)
#%%
textreuses = materialise_row_numbers(
    fname="textreuses",
    df=spark.sql("""
        SELECT 
            ti1.trs_id AS trs1_id,
            text1_text_start AS trs1_start,
            text1_text_end AS trs1_end,
            ti2.trs_id AS trs2_id,
            text2_text_start AS trs2_start,
            text2_text_end AS trs2_end,
            align_length,
            positives_percent
        FROM textreuses_raw t
        LEFT JOIN textreuse_ids ti1 ON t.text1_id = ti1.text_name
        LEFT JOIN textreuse_ids ti2 ON t.text2_id = ti2.text_name
        ORDER BY trs1_id,trs2_id
        """),
    col_name="textreuse_id",
    bucket=processed_bucket
)
#%%
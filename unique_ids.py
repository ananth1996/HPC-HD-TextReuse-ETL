#%%
from pathlib import Path
from time import perf_counter as time
import logging
import argparse
from pyspark.sql.functions import *
from spark_utils import *
import os
project_root = Path.cwd().resolve()
#%%
def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_s3_bucket",type=str,help="The s3 bucket where zip file is stored",default="textreuse-raw-data")
    parser.add_argument("--output_s3_bucket",type=str,help="The s3 buckets where processed files will be",default="textreuse-processed-data")
    parser.add_argument("--num_partitions",type=int,default=200,help="Number of partitions for spark")
    parser.add_argument(
    '-d', '--debug',
    help="Print lots of debugging statements",
    action="store_const", dest="loglevel", const=logging.DEBUG,
    default=logging.WARNING,
    )
    parser.add_argument(
        '-v', '--verbose',
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
    )
    return parser
#%%
#%%
# Basic Set up
args = get_parser().parse_args([])
logging.basicConfig(level=args.loglevel)
logger =  logging.getLogger(__name__)
#%%
textreuses_raw = get_s3(fname="txtreuse",bucket=args.input_s3_bucket,table_name="textreuses_raw")
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
#%%
import toml
from pathlib import Path
from time import perf_counter as time
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructField, StructType, LongType
from pyspark.sql.functions import *
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
def maybe_cache(table_name: str, df, cache: bool):
    if cache:
        df.createOrReplaceTempView(table_name + "_source")
        spark.sql("DROP TABLE IF EXISTS " + table_name)
        spark.sql("CACHE TABLE " + table_name + " AS TABLE " + table_name + "_source")
        return spark.sql("SELECT * FROM " + table_name)
    else:
        df.createOrReplaceTempView(table_name)
        return df

def s3_uri_exists(path: str):
    # create a file system for that bucket
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path), sc._jsc.hadoopConfiguration()
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))

def get_s3(
    fname: str,
    bucket: str,
    cache: bool = False,
    table_name: Optional[str] = None,
):
    if table_name is None:
        table_name = fname
    return maybe_cache(
        table_name, spark.read.parquet(f"s3a://{bucket}/{fname}.parquet"), cache=cache
    )

def delete_s3(
        fname:str,
        bucket:str,
):
    path = f"s3a://{bucket}/{fname}.parquet"
    path_uri =sc._jvm.java.net.URI.create(path)
    _path = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        path_uri, sc._jsc.hadoopConfiguration()
    )
    if fs.exists(_path):
        fs.delete(_path,True)
    
def materialise_s3(
    fname: str,
    df: DataFrame,
    bucket: str,
    cache: bool = False,
    table_name: Optional[str] = None,
):
    df.write.mode("overwrite").parquet(f"s3a://{bucket}/{fname}.parquet")
    return get_s3(fname, bucket, cache, table_name)


def materialise_s3_if_not_exists(
    fname: str,
    df: DataFrame,
    bucket: str,
    cache: bool = False,
    table_name: Optional[str] = None,
):
    if not s3_uri_exists(f"s3a://{bucket}/{fname}.parquet"):
        return materialise_s3(fname, df, bucket, cache, table_name)
    else:
        return get_s3(fname, bucket, cache, table_name)
#%%
# From https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex
def dfZipWithIndex (df, offset=1, col_name="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    new_schema = StructType(
                    [StructField(col_name,LongType(),True)]        # new added field in front
                    + df.schema.fields                            # previous schema
                )

    zipped_rdd = df.rdd.zipWithIndex()
    # use this for python 3+, tuple gets passed as single argument so using args and [] notation to read elements within args
    new_rdd = zipped_rdd.map(lambda args: ([args[1] + offset] + list(args[0])))      
    return spark.createDataFrame(new_rdd, new_schema)


def materialise_row_numbers(fname:str,df:DataFrame,col_name:str,bucket:str,table_name:Optional[str]=None):
    # materialise the dataframe if it was not materialised
    df = materialise_s3_if_not_exists(fname,df,bucket=bucket,table_name=table_name)

    # check if the row_number column already exists
    if col_name not in df.columns:
        # Write dataframe with row numbers to a temporary location
        df_tmp = materialise_s3(
            fname=f"{fname}_tmp",
            df=dfZipWithIndex(df,col_name=col_name),
            bucket=bucket
        )
        # then copy to correct location
        df = materialise_s3(fname=fname,df=df_tmp,bucket=bucket,table_name=table_name)
        # then drop temp location
        delete_s3(f"{fname}_tmp",bucket=bucket)
    
    return df
#%%
# Basic Set up
args = get_parser().parse_args([])
logging.basicConfig(level=args.loglevel)
logger =  logging.getLogger(__name__)
# set the interpreter to the poerty env
os.environ['PYSPARK_PYTHON'] = str(project_root/".venv/bin/python")
spark = (SparkSession
        .builder
        .appName("Unique IDs")
        .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext
# load credentials for Pouta s3 storage
with open(project_root/"s3credentials.toml","r") as fp:
    cred = toml.load(fp)
# set up credentials for spark
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", cred["default"]["aws_access_key_id"])
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", cred["default"]["aws_secret_access_key"])
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", cred["default"]["endpoint_url"])
#%%
textreuses_raw = get_s3(fname="txtreuse",bucket=args.input_s3_bucket,table_name="textreuses_raw")
ecco_core = get_s3(fname="ecco_core",bucket=args.input_s3_bucket)
eebo_tcp_core = get_s3(fname="eebo_tcp_core",bucket=args.input_s3_bucket)
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
    col_name="t_id",
    bucket = args.output_s3_bucket)
#%%
idmap = materialise_s3_if_not_exists(
    fname="idmap",
    df = spark.sql("""
    SELECT t_id,ecco_id,eebo_tcp_id FROM textreuse_ids ti 
    LEFT JOIN ecco_core ec ON ti.doc_name = ec.ecco_id
    LEFT JOIN eebo_tcp_core etc ON ti.doc_name = etc.eebo_tcp_id"""),
    bucket=args.output_s3_bucket)
#%%
textreuses = materialise_row_numbers(
    fname="textreuses",
    df=spark.sql("""
        SELECT 
            ti1.t_id AS t1_id,
            text1_text_start AS t1_start,
            text1_text_end AS t1_end,
            ti2.t_id AS t2_id,
            text2_text_start AS t2_start,
            text2_text_end AS t2_end,
            align_length,
            positives_percent
        FROM textreuses_raw t
        LEFT JOIN textreuse_ids ti1 ON t.text1_id = ti1.text_name
        LEFT JOIN textreuse_ids ti2 ON t.text2_id = ti2.text_name"""),
    col_name="tr_id",
    bucket=args.output_s3_bucket
)
#%%

#%%
# docs = spark.sql("""
#         SELECT ecco_id AS doc_name FROM ecco_core
#         UNION 
#         SELECT eebo_tcp_id AS doc_id FROM eebo_tcp_core
# """
# )
# docs.explain()
# docs = dfZipWithIndex(docs,colName="doc_id")
# idmapping = names.join(docs,on="doc_name")
# idmapping.tail(10)
# #%%
# ## Test 2
# names = spark.sql("""
# SELECT 
#     row_number() OVER(ORDER BY text_name) AS t_id,
#     text_name,
#     doc_name
# FROM (
#     SELECT text1_id AS text_name, SUBSTRING_INDEX(text1_id,".",1) AS doc_name  FROM textreuses
#     UNION
#     SELECT text2_id AS text_name, SUBSTRING_INDEX(text2_id,".",1) AS doc_name  FROM textreuses
# )
# """)
# names.explain()
# docs = spark.sql("""
# SELECT 
#     row_number() OVER(ORDER BY doc_name) as doc_id,
#     doc_name
# FROM(
#         SELECT ecco_id AS doc_name FROM ecco_core
#         UNION 
#         SELECT eebo_tcp_id AS doc_id FROM eebo_tcp_core
# )
# """
# )
# docs.explain()
# idmapping = names.join(docs,on="doc_name")
# idmapping.tail(10)
# %%

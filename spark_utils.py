from pyspark.sql import SparkSession
from pathlib import Path
project_root = Path(__file__).parent.resolve()
import toml
import os
from typing import *
from pyspark.sql.dataframe import DataFrame

def start_spark_app(project_root:Path,application_name:str="ETL"):
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
    return spark,sc

# get the spark sessions
spark,sc = start_spark_app(project_root=project_root)


def get_local(fname: str, cache: bool = False,table_name: Optional[str] = None):
    if table_name is None:
        table_name = fname
    return maybe_cache(table_name, spark.read.parquet(fname+".parquet"), cache=cache)
    
    
def materialise_local(name:str, df, cache: bool = False):
    df.write.mode("overwrite").option("compression","zstd").parquet(name+".parquet")
    return get_local(name, cache)

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
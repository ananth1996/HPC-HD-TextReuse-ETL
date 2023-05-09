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
            .appName("ETL")
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
    return register(table_name, spark.read.parquet(fname+".parquet"), cache=cache)
    
    
def materialise_local(name:str, df, cache: bool = False):
    df.write.mode("overwrite").option("compression","zstd").parquet(name+".parquet")
    return get_local(name, cache)

def register(table_name: str, df, cache: bool):
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
    return register(
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

def rename_s3(src_fname:str,dst_fname:str,bucket:str):
    _src_path = sc._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{src_fname}.parquet")
    _dst_path = sc._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{dst_fname}.parquet")
    path_uri = sc._jvm.java.net.URI.create(f"s3a://{bucket}")
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        path_uri, sc._jsc.hadoopConfiguration()
    )
    if (fs.exists(_src_path)):
        fs.rename(_src_path,_dst_path)

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
    _df = materialise_s3_if_not_exists(fname+"_tmp",df,bucket=bucket,table_name=table_name)

    # check if the row_number column already exists
    if col_name not in _df.columns:
        # Write dataframe with row numbers to a temporary location
        _df = materialise_s3(
            fname=fname,
            df=dfZipWithIndex(_df,col_name=col_name),
            bucket=bucket
        )
        # then drop temp location
        delete_s3(fname+"_tmp",bucket=bucket)
    else:
        # rename tmp as correct location
        rename_s3(fname+"_tmp",fname,bucket=bucket)
    
    return df
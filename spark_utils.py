from pathlib import Path
project_root = Path(__file__).parent.resolve()
import toml
import findspark
from typing import *
import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructField, StructType, LongType
def start_spark_app(project_root:Path,application_name:str="ETL"):
    os.environ['PYSPARK_PYTHON'] = str(project_root/".venv/bin/python")
    findspark.add_packages("graphframes:graphframes:0.8.2-spark3.2-s_2.12")
    findspark.init()
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
# the buckets
processed_bucket = "textreuse-processed-data"
raw_bucket = "textreuse-raw-data"
denorm_bucket = "textreuse-denormalized-data"

# the file systems for the buckets
processed_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(f"s3a://{processed_bucket}"), 
        sc._jsc.hadoopConfiguration()
    )
raw_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(f"s3a://{raw_bucket}"), 
        sc._jsc.hadoopConfiguration()
    )

denorm_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(f"s3a://{denorm_bucket}"), 
        sc._jsc.hadoopConfiguration()
    )

# create a mapping for easy access
fs_dict = {processed_bucket:processed_fs,raw_bucket:raw_fs,denorm_bucket:denorm_fs}

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

def s3_uri_exists(path: str,bucket:Optional[str]=None):
    if bucket is None:
        # create a file system for that bucket
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI.create(path), sc._jsc.hadoopConfiguration()
        )
    else:
        fs = fs_dict[bucket]
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
    _path = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_dict[bucket]
    if fs.exists(_path):
        fs.delete(_path,True)

def rename_s3(src_fname:str,dst_fname:str,bucket:str):
    _src_path = sc._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{src_fname}.parquet")
    _dst_path = sc._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{dst_fname}.parquet")
    fs = fs_dict[bucket]
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
    # check if row numbers already materialised
    if s3_uri_exists(f"s3a://{bucket}/{fname}.parquet",bucket=bucket):
        _df = get_s3(fname,bucket=bucket)
    else:
        # write with row numbers to the location
        _df = materialise_s3(
            fname=fname,
            df=dfZipWithIndex(df,col_name=col_name),
            bucket=bucket
        )
    return _df


def materialise_with_int_id(fname:str,df:DataFrame,col_name:str,id_col_name:str,bucket:str,keep_id_mapping:bool=True,id_fname:Optional[str]=None,drop_col:bool=True) -> DataFrame:
    """Creates INT ids for a column in dataframe and adds it back as id columns

    Args:
        fname (str): The filename to store the dataframe in
        df (DataFrame): The dataframe 
        col_name (str): The column for which to create integer IDS
        id_col_name (str): The column name for the new ID column
        bucket (str): The bucket where to store the data
        keep_id_mapping(bool): Whether to keep the INT id mapping file. Defaults to True
        id_fname(str): Optional. The name of id mapping file. Defaults to None.

    Returns:
        DataFrame: The dataframe with a new column with INT ids
    """
    if s3_uri_exists(f"s3a://{bucket}/{fname}.parquet"):
        rename_s3(fname,fname,bucket=bucket)
        # load dataframe 
        _df = get_s3(fname,bucket=bucket)
    else:
        register(fname,df,False)
        if id_fname is None:
            id_fname = col_name+"_id_mapping"
        
        # make INT ids for column
        id_df = materialise_row_numbers(
            fname=id_fname,
            df=spark.sql(f"""
                SELECT DISTINCT {col_name}
                FROM {fname}
                ORDER BY {col_name}
                """),
            col_name=id_col_name,
            bucket=bucket
            )
        # update the dataframe with the new mapping
        query =spark.sql(f"""
                SELECT orig.*,{id_col_name} FROM {fname} orig
                INNER JOIN {id_fname} USING ({col_name})
                """)
        if drop_col:
            query=query.drop(col_name) 
        # materialise the dataframe
        _df = materialise_s3(
            fname=fname, 
            df=query,
            bucket=bucket
            )
        if not keep_id_mapping:
            delete_s3(id_fname,bucket=bucket)
    
    return _df

with open(project_root/"database.toml") as fp:
    db_options = toml.load(fp)

def jdbc_opts(conn,db_options:dict=db_options):
    opts = db_options["mariadb"]
    return (conn
        .format("jdbc")
        .option("driver", opts["driver"])
        .option("url", opts["url"])
        .option("user", opts["user"])
        .option("password", opts["password"])
        .option("fetchsize",opts["fetchsize"])
        .option("batchsize",opts["batchsize"]))


# %%
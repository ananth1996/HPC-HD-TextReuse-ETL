from pathlib import Path
from dotenv import load_dotenv
project_root = Path(__file__).parent.parent.resolve()
import toml
import os
import findspark
load_dotenv()
findspark.init()
from typing import *
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructField, StructType, LongType
from etl_textreuse.database_utils import get_sqlalchemy_engine
from sqlalchemy import text
from time import perf_counter as time
# the buckets
processed_bucket = os.getenv("PROCESSED_BUCKET")
raw_bucket = os.getenv("RAW_BUCKET")

def get_spark_session(project_root:Path=project_root,application_name:str="ETL"):
    #findspark.add_packages("graphframes:graphframes:0.8.2-spark3.2-s_2.12"
    spark = (SparkSession
            .builder
            .appName(application_name)
            # To account for issue with historical dates 
            #    See https://docs.databricks.com/en/error-messages/inconsistent-behavior-cross-version-error-class.html#write_ancient_datetime
            .config("spark.sql.parquet.datetimeRebaseModeInWrite","CORRECTED")
            # .config("spark.hadoop.fs.s3a.ssl.channel.mode","openssl")
            # Send the zipfile of the archive to all workers to ensure they have the same Python environment
            .config("spark.archives",str(os.getenv("VENV_ZIP_FILE"))) # to send the environment to workers
            .config('spark.ui.showConsoleProgress', 'false')
            #.config('spark.graphx.pregel.checkpointInterval','1')
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
            .enableHiveSupport()
            .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    # set up credentials for spark
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY"))
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.getenv("AWS_ENDPOINT_URL"))
    sc.setCheckpointDir(f"s3a://{processed_bucket}/checkpoints")

    return spark


def get_local(spark:SparkSession,fname: str, cache: bool = False,table_name: Optional[str] = None):
    if table_name is None:
        table_name = fname
    return register(spark, table_name, spark.read.parquet(fname+".parquet"), cache=cache)
    
    
def materialise_local(spark:SparkSession, name:str, df, cache: bool = False):
    df.write.mode("overwrite").option("compression","zstd").parquet(name+".parquet")
    return get_local(spark, name, cache)

def register(spark: SparkSession, table_name: str, df, cache: bool):
    if cache:
        df.createOrReplaceTempView(table_name + "_source")
        spark.sql("DROP TABLE IF EXISTS " + table_name)
        spark.sql("CACHE TABLE " + table_name + " AS TABLE " + table_name + "_source")
        return spark.sql("SELECT * FROM " + table_name)
    else:
        df.createOrReplaceTempView(table_name)
        return df

def get_hadoop_filesystem(spark:SparkSession,path:str):
    sc = spark.sparkContext
    # create a file system for that bucket
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path), sc._jsc.hadoopConfiguration()
    )
    return fs

def get_hadoop_path(spark:SparkSession,path:str):
    return spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)

def s3_uri_exists(spark:SparkSession, path: str):
    fs = get_hadoop_filesystem(spark,path)    
    return fs.exists(get_hadoop_path(spark,path))

def get_s3(
    spark: SparkSession,
    fname: str,
    bucket: str,
    cache: bool = False,
    table_name: Optional[str] = None,
):
    if table_name is None:
        table_name = fname
    return register(
        spark, table_name, spark.read.parquet(f"s3a://{bucket}/{fname}.parquet"), cache=cache
    )

def delete_s3(
        spark: SparkSession,
        fname:str,
        bucket:str,
):
    path = f"s3a://{bucket}/{fname}.parquet"
    fs = get_hadoop_filesystem(spark,path)
    _path = get_hadoop_path(path)
    if fs.exists(_path):
        fs.delete(_path,True)

def rename_s3(spark:SparkSession,src_fname:str,dst_fname:str,bucket:str):
    _src_path = get_hadoop_path(spark,f"s3a://{bucket}/{src_fname}.parquet")
    _dst_path = get_hadoop_path(spark,f"s3a://{bucket}/{dst_fname}.parquet")
    fs = get_hadoop_filesystem(spark,f"s3a://{bucket}/{src_fname}.parquet")
    if (fs.exists(_src_path)):
        fs.rename(_src_path,_dst_path)

def materialise_s3(
    spark:SparkSession,
    fname: str,
    df: DataFrame,
    bucket: str,
    cache: bool = False,
    table_name: Optional[str] = None,
):
    df.write.mode("overwrite").parquet(f"s3a://{bucket}/{fname}.parquet")
    return get_s3(spark, fname, bucket, cache, table_name)


def materialise_s3_if_not_exists(
    spark:SparkSession,
    fname: str,
    df: DataFrame,
    bucket: str,
    cache: bool = False,
    table_name: Optional[str] = None,
):
    if not s3_uri_exists(spark,f"s3a://{bucket}/{fname}.parquet"):
        return materialise_s3(spark,fname, df, bucket, cache, table_name)
    else:
        return get_s3(spark,fname, bucket, cache, table_name)
    
#%%
# From https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex
def dfZipWithIndex (spark:SparkSession, df, offset=1, col_name="rowId"):
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


def materialise_row_numbers(spark:SparkSession,fname:str,df:DataFrame,col_name:str,bucket:str,table_name:Optional[str]=None):
    # check if row numbers already materialised
    # if s3_uri_exists(spark,f"s3a://{bucket}/{fname}.parquet"):
    #     _df = get_s3(spark,fname,bucket=bucket)
    # else:
    # write with row numbers to the location
    _df = materialise_s3(
        spark,
        fname=fname,
        df=dfZipWithIndex(spark,df,col_name=col_name),
        bucket=bucket
    )
    return _df


def materialise_with_int_id(spark:SparkSession,fname:str,df:DataFrame,col_name:str,id_col_name:str,bucket:str,keep_id_mapping:bool=True,id_fname:Optional[str]=None,drop_col:bool=True) -> DataFrame:
    """Creates INT ids for a column in dataframe and adds it back as id columns

    Args:
        fname (str): The filename to store the dataframe in
        df (DataFrame): The dataframe 
        col_name (str): The column for which to create integer IDS
        id_col_name (str): The column name for the new ID column
        bucket (str): The bucket where to store the data
        keep_id_mapping(bool): Whether to keep the INT id mapping file. Defaults to True
        id_fname(str): Optional. The name of id mapping file. Defaults to None.
        drop_col(bool): Optional. Whether to drop col_name from resulting dataframe. Defaults to True.

    Returns:
        DataFrame: The dataframe with a new column with INT ids
    """
    # if s3_uri_exists(spark,f"s3a://{bucket}/{fname}.parquet"):
    #     rename_s3(spark,fname,fname,bucket=bucket)
    #     # load dataframe 
    #     _df = get_s3(spark,fname,bucket=bucket)
    # else:
    register(spark,fname,df,False)
    if id_fname is None:
        id_fname = col_name+"_id_mapping"
    
    # make INT ids for column
    id_df = materialise_row_numbers(
        spark,
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
        spark,
        fname=fname, 
        df=query,
        bucket=bucket
        )
    if not keep_id_mapping:
        delete_s3(spark,id_fname,bucket=bucket)

    return _df


def jdbc_opts(conn):
    url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}?permitMysqlScheme"
    return (conn
        .format("jdbc")
        .option("driver", os.getenv("DB_DRIVER"))
        .option("url", url)
        .option("user", os.getenv("DB_USERNAME"))
        .option("password", os.getenv("DB_PASSWORD"))
        .option("fetchsize",os.getenv("DB_FETCHSIZE"))
        .option("batchsize",os.getenv("DB_BATCHSIZE")))


def load_table(spark:SparkSession,table:str,bucket:str,database:str,schema:str,index:str,table_name:Optional[str]=None) -> Dict[str,float]:
    # load spark table
    df = get_s3(spark,table,bucket)
    engine = get_sqlalchemy_engine()
    start = time()
    if table_name is None:
        table_name = table
    with engine.connect() as conn:
        # drop table if present
        conn.execute(text(f"DROP TABLE IF EXISTS  {table_name}"))
        # create table with schema
        conn.execute(text(schema))
        print("Loading table into database")
        # load the table
        (
            jdbc_opts(df.write)
            .option("dbtable", table_name) 
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
        load_time = time() - start
        print("Checking Sizes")
        # check row counts
        database_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchall()[0][0]
        spark_count = df.count()
        assert database_count == spark_count
        print("Creating Index")
        start = time()
        conn.execute(text(index))
        index_time = time() - start
    metadata = {"rows":database_count,"Loading Time":load_time,"Indexing Time":index_time}
    return metadata
# %%
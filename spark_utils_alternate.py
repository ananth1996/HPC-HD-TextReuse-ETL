from pathlib import Path
project_root = Path(__file__).parent.resolve()
import toml
import findspark
from typing import *
import os
os.environ['PYSPARK_PYTHON'] = str(project_root/".venv/bin/python")
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructField, StructType, LongType

# the buckets
BUCKETS_MAP = {
    "hpc-hd": {
        "processed_bucket": "textreuse-processed-data",
        "raw_bucket": "textreuse-raw-data",
        "denorm_bucket": "textreuse-denormalized-data",
    },
    "hpc-hd-newspapers": {
        "processed_bucket": "textreuse-dagster-rahti",
        "raw_bucket": "textreuse-raw-data",
        "denorm_bucket": "textreuse-dagster-rahti",
    }
}

TABLES_MAP ={
    "hpc-hd":[
    ("textreuse_ids","processed_bucket"),
    ("manifestation_ids","processed_bucket"),
    ("edition_ids","processed_bucket"),
    ("work_ids","processed_bucket"),
    ("actor_ids","processed_bucket"),
    ("textreuse_work_mapping","processed_bucket"),
    ("textreuse_edition_mapping","processed_bucket"),
    ("work_mapping","processed_bucket"),
    ("edition_mapping","processed_bucket"),
    ("edition_publication_year","processed_bucket"),
    ("work_earliest_publication_year","processed_bucket"),
    ("textreuse_earliest_publication_year","processed_bucket"),
    ("edition_authors","processed_bucket"),
    ("estc_core","raw_bucket"),
    ("ecco_core","raw_bucket"),
    ("eebo_core","raw_bucket"),
    ("newspapers_core","raw_bucket"),
    ("estc_actor_links","raw_bucket"),
    ("estc_actors","raw_bucket"),
    ("defrag_pieces","processed_bucket"),
    ("defrag_textreuses","processed_bucket"),
    ("clustered_defrag_pieces","processed_bucket"),
    ("earliest_textreuse_by_cluster","processed_bucket"),
    ("earliest_work_and_pieces_by_cluster","processed_bucket"),
    ("reception_edges_denorm","denorm_bucket"),
    ("source_piece_statistics_denorm","denorm_bucket"),
    ("coverages","processed_bucket"),
    ],
    "hpc-hd-newspapers":[
    ("textreuse_ids","processed_bucket"),
    ("manifestation_ids","processed_bucket"),
    ("edition_ids","processed_bucket"),
    ("work_ids","processed_bucket"),
    ("actor_ids","processed_bucket"),
    ("textreuse_work_mapping","processed_bucket"),
    ("textreuse_edition_mapping","processed_bucket"),
    ("work_mapping","processed_bucket"),
    ("edition_mapping","processed_bucket"),
    ("edition_publication_date","processed_bucket"),
    ("work_earliest_publication_date","processed_bucket"),
    ("textreuse_earliest_publication_date","processed_bucket"),
    ("textreuse_source_lengths","processed_bucket"),
    ("edition_authors","processed_bucket"),
    ("estc_core","raw_bucket"),
    ("ecco_core","raw_bucket"),
    ("eebo_core","raw_bucket"),
    ("newspapers_core","raw_bucket"),
    ("estc_actor_links","raw_bucket"),
    ("estc_actors","raw_bucket"),
    ("defrag_pieces","processed_bucket"),
    ("defrag_textreuses","processed_bucket"),
    ("clustered_defrag_pieces","processed_bucket"),
    ("earliest_textreuse_by_cluster","processed_bucket"),
    ("earliest_work_and_pieces_by_cluster","processed_bucket"),
    ("reception_edges_denorm","denorm_bucket"),
    ("source_piece_statistics_denorm","denorm_bucket"),
    ("coverages","processed_bucket"),
    # ("textreuse_sources","processed_bucket"),
    ]
} 


def get_spark_session(application_name:str="ETL",project_root:Path=project_root):
    # 
    # #findspark.add_packages("graphframes:graphframes:0.8.2-spark3.2-s_2.12")
    # 
    spark = (SparkSession
            .builder
            .appName(application_name)
            # To account for issue with historical dates 
            # See https://docs.databricks.com/en/error-messages/inconsistent-behavior-cross-version-error-class.html#write_ancient_datetime
            .config("spark.sql.parquet.datetimeRebaseModeInWrite","CORRECTED")
            # .config("spark.hadoop.fs.s3a.ssl.channel.mode","openssl")
            #.config('spark.ui.showConsoleProgress', 'false')
            #.config('spark.graphx.pregel.checkpointInterval','1')
            # .config("spark.driver.memory","10g")
            # .config("spark.sql.warehouse.dir","/Users/mahadeva/spark-warehouse")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
            # .enableHiveSupport()
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
    #checkpoint_dir = project_root/"checkpoints"
    #checkpoint_dir.mkdir(exist_ok=True,parents=True)
    #sc.setCheckpointDir(str(checkpoint_dir))
    # sc.setCheckpointDir(f"s3a://{processed_bucket}/checkpoints")

    return spark

# # get the spark sessions
# spark,sc = start_spark_app(project_root=project_root)

# # the file systems for the buckets
# processed_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
#         sc._jvm.java.net.URI.create(f"s3a://{processed_bucket}"), 
#         sc._jsc.hadoopConfiguration()
#     )
# raw_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
#         sc._jvm.java.net.URI.create(f"s3a://{raw_bucket}"), 
#         sc._jsc.hadoopConfiguration()
#     )

# denorm_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
#         sc._jvm.java.net.URI.create(f"s3a://{denorm_bucket}"), 
#         sc._jsc.hadoopConfiguration()
#     )

# # create a mapping for easy access
# fs_dict = {processed_bucket:processed_fs,raw_bucket:raw_fs,denorm_bucket:denorm_fs}

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


def get_filesize(spark:SparkSession,path:str):
    fs = get_hadoop_filesystem(spark,path)
    return fs.getContentSummary(get_hadoop_path(spark,path)).getLength()

def get_s3_parquet_size(spark,fname,bucket):
    path = f"s3a://{bucket}/{fname}.parquet"
    return get_filesize(spark,path)

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

# with open(project_root/"database.toml") as fp:
#     db_options = toml.load(fp)

# def jdbc_opts(conn,database:str,db_options:dict=db_options):
#     opts = db_options[database]
#     return (conn
#         .format("jdbc")
#         .option("driver", opts["driver"])
#         .option("url", opts["url"])
#         .option("user", opts["user"])
#         .option("password", opts["password"])
#         .option("fetchsize",opts["fetchsize"])
#         .option("batchsize",opts["batchsize"]))
# %%
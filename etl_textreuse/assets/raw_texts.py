#%%
from dagster import asset,  AssetKey, AssetSpec
import boto3 
import zipfile
from pathlib import Path
import smart_open
from pyspark.sql import SparkSession
import os
from functools import partial
from etl_textreuse.spark_utils import *
from etl_textreuse.assets.raw_textreuses import process_partition
from pyspark.sql.types import *
project_root = Path(__file__).parent.resolve()
#%% 

textreuse_sources_zip_file = AssetSpec(key=AssetKey(
    "textreuse_sources_zip_file"), group_name="upstream_metadata", description="The sources of all textreuses")



@asset(
    deps=[textreuse_sources_zip_file],
    description="The parquet file of raw text for every document",
    group_name="metadata"
)
def textreuse_sources() -> None:
    # get the name of the raw texts zip file
    fname =os.environ["RAW_TEXTS_ZIP_FILE"]
    num_partitions = 200
    # load credentials for Pouta s3 storage
    cred = {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_KEY"],
        "endpoint_url": os.environ["AWS_ENDPOINT_URL"],
    }

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        **cred
    )
    transport_params = dict(client=s3)
    # filename inside the bucket
    fname = f"s3://{raw_bucket}/{fname}"
    # filename inside the bucket
    output_fname = f"s3a://{processed_bucket}/textreuse_sources.parquet"
    # read the files in the zip file
    with smart_open.open(fname, "rb", compression='disable', transport_params=transport_params) as fileobj:
        # open file as ZIP
        with zipfile.ZipFile(fileobj) as zf:
            infolist = zf.infolist()

    # get spark session
    spark = get_spark_session(
        project_root, application_name="Raw Texts")
    # get a partial function
    func1 = partial(process_partition, fname=fname, cred=cred)
    rdd1 = spark.sparkContext.parallelize(infolist, num_partitions)
    # get the JSONs from the RDD
    jsons1 = rdd1.mapPartitions(func1)
    # the schema for the dataframe
    schema = StructType(
        [
            StructField("doc_id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("collection", StringType(), True),
            StructField("text_loc", StringType(), True),
        ]
    ) 
    # convert into a dataframe
    df1 = spark.createDataFrame(jsons1, schema=schema)
    # write to the output file
    df1.write.mode("overwrite").parquet(output_fname)
#%%

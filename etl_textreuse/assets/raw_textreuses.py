#%%
import boto3 
import zipfile
import toml
from pathlib import Path
import smart_open
import logging
from functools import partial
from pyspark.sql.types import *
from etl_textreuse.spark_utils import *
project_root = Path(__file__).parent.resolve()
import json
from dagster import asset,  AssetKey, SourceAsset
#%%
logger = logging.getLogger(__name__)

def process_file(fileinfo:zipfile.ZipInfo,handler:zipfile.ZipFile):
    """Reads a single file and returns JSON objects in stream

    Args:
        fileinfo (zipfile.ZipInfo): The file inside the zip to process
        handler (zipfile.ZipFile): The handler for the zipfile object

    Yields:
        dict: JSON objects
    """

    with handler.open(fileinfo) as json_fp:
        try:
            logger.info(f"Opening json file: {json_fp}")
            counter = 0
            # read JSON objects in streams
            for line in json_fp:
                counter+=1 
                logger.debug(f"Returning item: {counter}")
                item = json.loads(line)
                yield item
        except Exception:
            logger.exception(f"Cannot Open ZipFile object: {fileinfo}")
    

def process_partition(iterator,fname:str,cred:dict):
    """Processes a chunk of files inside the zip.
    Initializes a s3 stream for a spark partition to read sequentially.

    Args:
        iterator : spark iterator for a partition
        fname (str): The name of the s3 file to open
        cred (dict): The credentials
    Yields:
        dict: JSON objects from the files
    """
    # open a s3 session
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        **cred["default"]
    )
    transport_params=dict(client=s3)
    # open the S3 stream
    with smart_open.open(fname,"rb",compression='disable',transport_params=transport_params) as fileobj:
        # open it as a ZipFile
        with zipfile.ZipFile(fileobj) as zf:
            for fileinfo in iterator:
                logger.info(f"Processing file {fileinfo}")
                for json_object in process_file(fileinfo,zf):
                    yield json_object



zip_file = SourceAsset(key=AssetKey("raw_texreuses_zip"),description="The raw zip files with textreuse data")


@asset(deps=[zip_file],description="The parquet file of raw textreuses")
def raw_textreuses() -> None:
    fname = "newspapers_run_samples.zip"
    num_partitions = 200
    # load credentials for Pouta s3 storage
    with open(project_root/"s3credentials.toml","r") as fp:
        cred = toml.load(fp)

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        **cred["default"]
    )
    transport_params=dict(client=s3)
    # filename inside the bucket
    fname = f"s3://{raw_bucket}/{fname}"
    # filename inside the bucket
    output_fname = f"s3a://{processed_bucket}/raw_textreuses.parquet"
    # read the files in the zip file
    with smart_open.open(fname,"rb",compression='disable',transport_params=transport_params) as fileobj:
        # open file as ZIP
        with zipfile.ZipFile(fileobj) as zf:
            infolist = zf.infolist()

    # get spark session
    spark = get_spark_session(project_root,application_name="Raw Text Reuses Zip2Parquet")
    # get a partial function
    func1 = partial(process_partition,fname=fname,cred=cred)
    rdd1 = spark.sparkContext.parallelize(infolist,num_partitions)
    # get the JSONs from the RDD
    jsons1 = rdd1.mapPartitions(func1)
    # the schema for the dataframe
    schema = StructType(
        [
            StructField("align_length", IntegerType(), True),
            StructField("positives_percent", FloatType(), True),
            StructField("text1_id", StringType(), True),
            StructField("text1_text", StringType(), True),
            StructField("text1_text_end", IntegerType(), True),
            StructField("text1_text_start", IntegerType(), True),
            StructField("text2_id", StringType(), True),
            StructField("text2_text", StringType(), True),
            StructField("text2_text_end", IntegerType(), True),
            StructField("text2_text_start", IntegerType(), True),
        ]
    )
    # convert into a dataframe
    df1 = spark.createDataFrame(jsons1,schema=schema)
    # write to the output file
    df1.write.mode("overwrite").parquet(output_fname)


# %%
from dagster import asset,  AssetKey, SourceAsset
import json
import boto3
import zipfile
import toml
from pathlib import Path
import smart_open
import logging
from functools import partial
from pyspark.sql.types import *
from etl_textreuse.spark_utils import *
project_root = Path(__file__).parent.parent.parent.resolve()
# %%
logger = logging.getLogger(__name__)


def process_file(fileinfo: zipfile.ZipInfo, handler: zipfile.ZipFile):
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
                counter += 1
                logger.debug(f"Returning item: {counter}")
                item = json.loads(line)
                yield item
        except Exception:
            logger.exception(f"Cannot Open ZipFile object: {fileinfo}")


def process_partition(iterator, fname: str, cred: dict):
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
    transport_params = dict(client=s3)
    # open the S3 stream
    with smart_open.open(fname, "rb", compression='disable', transport_params=transport_params) as fileobj:
        # open it as a ZipFile
        with zipfile.ZipFile(fileobj) as zf:
            for fileinfo in iterator:
                logger.info(f"Processing file {fileinfo}")
                for json_object in process_file(fileinfo, zf):
                    yield json_object


zip_file = SourceAsset(key=AssetKey("raw_texreuses_zip"),
                       description="The raw zip files with textreuse data",group_name="textreuses")


@asset(
    deps=[zip_file],
    description="The parquet file of raw textreuses",
    group_name="textreuses"
)
def raw_textreuses() -> None:
    fname = "tr_data_out_all.zip"
    num_partitions = 200
    # load credentials for Pouta s3 storage
    with open(project_root/"s3credentials.toml", "r") as fp:
        cred = toml.load(fp)

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        **cred["default"]
    )
    transport_params = dict(client=s3)
    # filename inside the bucket
    fname = f"s3://{raw_bucket}/{fname}"
    # filename inside the bucket
    output_fname = f"s3a://{processed_bucket}/raw_textreuses.parquet"
    # read the files in the zip file
    with smart_open.open(fname, "rb", compression='disable', transport_params=transport_params) as fileobj:
        # open file as ZIP
        with zipfile.ZipFile(fileobj) as zf:
            infolist = zf.infolist()

    # get spark session
    spark = get_spark_session(
        project_root, application_name="Raw Text Reuses Zip2Parquet")
    # get a partial function
    func1 = partial(process_partition, fname=fname, cred=cred)
    rdd1 = spark.sparkContext.parallelize(infolist, num_partitions)
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
    df1 = spark.createDataFrame(jsons1, schema=schema)
    # write to the output file
    df1.write.mode("overwrite").parquet(output_fname)


@asset(
    deps=[raw_textreuses],
    description="The INT ids for textreuse sources",
    group_name="textreuses"
)
def textreuse_ids() -> None:
    spark = get_spark_session(project_root, application_name="textreuse_ids")
    # load the
    get_s3(spark, "raw_textreuses", processed_bucket,
           table_name="textreuses_raw")
    textreuse_ids = materialise_row_numbers(
        spark,
        fname="textreuse_ids",
        df=spark.sql("""   
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
        bucket=processed_bucket)


@asset(
    deps=[raw_textreuses, textreuse_ids],
    description="The textreuses with ids mapped",
    group_name="textreuses"
)
def textreuses() -> None:
    spark = get_spark_session(project_root, application_name="textreuses")
    # load the required files
    get_s3(spark, "raw_textreuses", processed_bucket,
           table_name="textreuses_raw")
    get_s3(spark, "textreuse_ids", processed_bucket)

    textreuses = materialise_row_numbers(
        spark,
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

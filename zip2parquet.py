#%%
import boto3 
import zipfile
import toml
from pathlib import Path
import tarfile
import smart_open
import logging
import argparse
from pyspark.sql import SparkSession
import os
import ijson 
from functools import partial
project_root = Path(__file__).parent.resolve()
#%% 

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("fname",help="Name of the file to process",type=str)
    parser.add_argument("--local",help="Whether to use local file",action="store_true")
    parser.add_argument("--s3_bucket",type=str,help="The s3 bucket where zip file is stored",default="textreuse-raw-data")
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
def process_file(fileinfo:zipfile.ZipInfo,handler:zipfile.ZipFile):
    """Reads a single file and returns JSON objects in stream

    Args:
        fileinfo (zipfile.ZipInfo): The file inside the zip to process
        handler (zipfile.ZipFile): The handler for the zipfile object

    Yields:
        dict: JSON objects
    """

    with handler.open(fileinfo) as fp:
        try:
            # open tar.gz file
            logger.info(f"Opening tar file {fp}")
            with tarfile.open(fileobj=fp,debug=3,mode="r|gz") as tf:
                # go through members in tarfile
                logger.debug("Looping over members")
                try:
                    for member in tf:
                        logger.debug(f"{member=}")
                        logger.debug(f"{tf.fileobj=}, {member.offset_data=},{member.size=}, {member.sparse=}")
                        # extract json from tarfile 
                        counter = 0
                        with tf.extractfile(member) as json_fp:
                            # read JSON objects in streams
                            for item in ijson.items(json_fp,prefix="item"):
                                counter+=1 
                                logger.debug(f"Returning item: {counter}")
                                yield item
                except Exception:
                    logger.exception(f"Cannot process TarFile: {fp.name}")
        except Exception:
            logger.exception(f"Cannot Open ZipFile object: {fileinfo}")
    

def process_partition(iterator,fname:str,cred:dict,loglevel:int):
    """Processes a chunk of files inside the zip.
    Initializes a s3 stream for a spark partition to read sequentially.

    Args:
        iterator : spark iterator for a partition
        fname (str): The name of the s3 file to open
        cred (dict): The credentials
        loglevel (int): The logging level

    Yields:
        dict: JSON objects from the files
    """
    logging.basicConfig(level=loglevel)
    logging.info("In partition")
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

#%%

if __name__ == "__main__":
        
    # Basic Set up
    args = get_parser().parse_args()
    logging.basicConfig(level=args.loglevel)
    logger =  logging.getLogger(__name__)
    # set the interpreter to the poerty env
    os.environ['PYSPARK_PYTHON'] = str(project_root/".venv/bin/python")
    spark = (SparkSession
            .builder
            .appName("ETL")
            .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    if args.local:
        logger.info("Using local file")
        fname = args.fname
        transport_params = dict()
        output_fname = f"{args.fname.split('.')[0]}.parquet"
    else:
        logger.info("Using S3 file")
        # load credentials for Pouta s3 storage
        with open(project_root/"s3credentials.toml","r") as fp:
            cred = toml.load(fp)
        # set up credentials for spark
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", cred["default"]["aws_access_key_id"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", cred["default"]["aws_secret_access_key"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", cred["default"]["endpoint_url"])
        # create a s3 session for boto3
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            **cred["default"]
        )
        transport_params=dict(client=s3)
        # filename inside the bucket
        fname = f"s3://{args.s3_bucket}/{args.fname}"
        # filename inside the bucket
        output_fname = f"s3a://{args.s3_bucket}/{args.fname.split('.')[0]}.parquet"

    # read the files in the zip file
    with smart_open.open(fname,"rb",compression='disable',transport_params=transport_params) as fileobj:
        # open file as ZIP
        with zipfile.ZipFile(fileobj) as zf:
            infolist = zf.infolist()
    
    # get a partial function
    func1 = partial(process_partition,fname=fname,cred=cred,loglevel=args.loglevel)
    rdd1 = sc.parallelize(infolist,args.num_partitions)
    # get the JSONs from the RDD
    jsons1 = rdd1.mapPartitions(func1)
    # convert into a dataframe
    df1 = jsons1.toDF()
    # write to the output file
    df1.write.mode("overwrite").parquet(output_fname)
    #%%

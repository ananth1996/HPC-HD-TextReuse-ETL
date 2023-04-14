#%%
import boto3 
import io 
import zipfile
import toml
from pathlib import Path
import tarfile
import json
from time import perf_counter as time
import smart_open
import logging
import argparse
import random
project_root = Path(__file__).parent.resolve()
#%%

# import findspark
# import os
# findspark.init()
# from pyspark.sql import SparkSession
# os.environ["JAVA_HOME"] = "/appl/soft/bio/java/openjdk/jdk8u312-b07/"
# spark = (SparkSession
# 		.builder
#         .master(f"local[1]")
# 		.appName("ETL")
# 		.config("spark.driver.memory","1g")
# 		# .config("spark.local.dir",os.environ["LOCAL_SCRATCH"])
# 		.getOrCreate())
# spark.sparkContext.setLogLevel("WARN")
# sc = spark.sparkContext
def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("fname",help="Name of the file to process",type=str)
    parser.add_argument("--local",help="Whether to use local file",action="store_true")
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
    parser.add_argument("--shuffle",action="store_true")
    parser.add_argument("--seed",type=int)

    return parser

#%%
if __name__ == "__main__":
    
    args = get_parser().parse_args()
    logging.basicConfig(level=args.loglevel)
    logger =  logging.getLogger(__name__)
    if args.local:
        logger.info("Using local file")
        fname = args.fname
        transport_params = dict()
    else:
        logger.info("Using S3 file")
        with open(project_root/"s3credentials.toml","r") as fp:
            cred = toml.load(fp)

        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            **cred["default"]
        )
        fname = f"s3://textreuse_raw_data/{args.fname}"
        transport_params=dict(client=s3)
    # start = time()
    logger.info("Starting Process")
    # with a smart open
    with smart_open.open(fname,"rb",compression='disable',transport_params=transport_params) as fileobj:
        # open file as ZIP
        with zipfile.ZipFile(fileobj) as zf:
            # go over all files in ZIP archive
            logger.debug("Looping over zip files")
            infolist = zf.infolist()
            if args.shuffle:
                random.seed(args.seed)
                random.shuffle(infolist)
            for i,fileinfo in enumerate(infolist):
                # open zip file
                with zf.open(fileinfo) as fp:
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
                                with tf.extractfile(member) as json_fp:
                                    # read the JSON
                                    json_str = json_fp.read()
                                    # # convert it to RDD
                                    # jsonRDD = sc.parallelize(json_str)
                                    # # load it as DataFrame
                                    # https://stackoverflow.com/questions/35770486/spark-streaming-json-to-parquet
                                    # df = spark.read.json(jsonRDD)
                                    # # write it out 
                                    # df.write.parquet(f"{member.name}.parquet")
                        except Exception:
                            logger.exception(f"Cannot process TarFile: {fp.name}")
#%%
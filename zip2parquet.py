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
from pyspark.sql import SparkSession
import os
import ijson 
from functools import partial
project_root = Path(__file__).resolve()
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
def get_jsons(fileinfo,fname:str,cred:dict):
    if "_fileobj" not in globals():
        logger.info("Creating global variables")
        global _fileobj
        global _session
        global _s3
        global _transport_params
        global _zf
        # _fileobj = 1
        _session = boto3.session.Session()
        _s3 = session.client(
            service_name='s3',
            **cred["default"]
        )
        # _fileobj =
        _fileobj = smart_open.open(fname,"rb",compression='disable',transport_params=dict(client=_s3))
        _zf = zipfile.ZipFile(_fileobj)
    with _zf.open(fileinfo) as fp:
        try:
            # open tar.gz file
            logger.info(f"Opening tar file {fp}")
            with tarfile.open(fileobj=fp,debug=3,mode="r|*") as tf:
                # go through members in tarfile
                logger.debug("Looping over members")
                try:
                    for member in tf:
                        logger.debug(f"{member=}")
                        logger.debug(f"{tf.fileobj=}, {member.offset_data=},{member.size=}, {member.sparse=}")
                        # extract json from tarfile 
                        with tf.extractfile(member) as json_fp:
                            # read JSON objects in streams
                            for item in ijson.items(json_fp,prefix="item"):
                                yield item
                except Exception:
                    logger.exception(f"Cannot process TarFile: {fp.name} members")
        except Exception:
            logger.exception(f"Cannot Open ZipFile object: {fileinfo}")

#%%

if __name__ == "__main__":
        
    # Basic Set up
    args = get_parser().parse_args(["txtreuse.zip","-v"])
    logging.basicConfig(level=args.loglevel)
    logger =  logging.getLogger(__name__)
    # set the interpreter to the poerty env
    os.environ['PYSPARK_PYTHON'] = "/home/jovyan/work/etl_textreuse/.venv/bin/python"
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
    func = partial(get_jsons,fname=fname,cred=cred)
    rdd = sc.parallelize(infolist,args.num_partitions)
    jsons = rdd.flatMap(func)
    df = jsons.toDF()
    df.write.mode("overwrite").parquet(output_fname)

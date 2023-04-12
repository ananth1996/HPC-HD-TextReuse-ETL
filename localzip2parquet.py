#%%
import boto3 
import io 
import zipfile
import toml
from pathlib import Path
import tarfile
import json
from time import perf_counter as time
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
#%%

if __name__ == "__main__":
    #%%

    #%%
    start = time()
    # open S3 file as ZIP
    with zipfile.ZipFile("test.zip") as zf:
        # go over all files in ZIP archive
        for i,fileinfo in enumerate(zf.infolist()):
            # open zip file
            with zf.open(fileinfo) as fp:
                print(fp)
                # open tar.gz file
                with tarfile.open(fileobj=fp,debug=3) as tf:
                    # go through members in tarfile
                    for member in tf:
                        print(f"{member=}")
                        print(tf.fileobj, member.offset_data,member.size, member.sparse)
                        # extract json from tarfile 
                        with tf.extractfile(member) as json_fp:
                            print(f"{json_fp=}")
                            break
                            # read the JSON 
                            # json_str = json_fp.read()
                            # convert it to RDD
                            # print(json_str)
                            # jsonRDD = sc.parallelize(json_str)
                            # load it as DataFrame
                            # df = spark.read.json(jsonRDD)
                            # write it out 
                            # df.write.parquet(f"{member.name}.parquet")
            break   
    elapsed = time() - start
    print(f"Took {elapsed:g} sec") 
#%%

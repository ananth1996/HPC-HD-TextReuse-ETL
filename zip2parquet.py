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

# From https://alexwlchan.net/2019/working-with-large-s3-objects/
class S3File(io.RawIOBase):
    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.position = 0

    def __repr__(self):
        return "<%s s3_object=%r>" % (type(self).__name__, self.s3_object)

    @property
    def size(self):
        return self.s3_object.content_length

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        print(f"seeking {offset=} {whence=}")
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise ValueError("invalid whence (%r, should be %d, %d, %d)" % (
                whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END
            ))

        return self.position

    def seekable(self):
        return True

    def read(self, size=-1):
        if size == -1:
            # Read to the end of the file
            range_header = "bytes=%d-" % self.position
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.position + size

            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()

            range_header = "bytes=%d-%d" % (self.position, new_position - 1)
            self.seek(offset=size, whence=io.SEEK_CUR)

        return self.s3_object.get(Range=range_header)["Body"].read()

    def readable(self):
        return True

#%%

if __name__ == "__main__":
    with open(project_root/"s3credentials.toml","r") as fp:
        cred = toml.load(fp)

    session = boto3.session.Session()
    s3 = session.resource(
        service_name='s3',
        **cred["default"]
    )
    s3_object = s3.Object(bucket_name="textreuse_raw_data", key="test.zip")
    s3_file = S3File(s3_object)
    #%%

    #%%
    start = time()
    # open S3 file as ZIP
    print("Starting Process")
    with zipfile.ZipFile(s3_file) as zf:
        # go over all files in ZIP archive
        print("Looping over zip files")
        for i,fileinfo in enumerate(zf.infolist()):
            # open zip file
            print(f"Opening Zip File {fileinfo}")
            with zf.open(fileinfo) as fp:
                # open tar.gz file
                print(f"Opening tar file {fp}")
                with tarfile.open(fileobj=fp,debug=3,mode="r:gz") as tf:
                    # go through members in tarfile
                    print("Looping over members")
                    for member in tf:
                        print(f"{member=}")
                        print(tf.fileobj, member.offset_data,member.size, member.sparse)
                        # extract json from tarfile 
                        with tf.extractfile(member) as json_fp:
                            print(f"{json_fp=}")
                            # break
                            # read the JSON 
                            json_str = json_fp.read(member.size)
                            # # convert it to RDD
                            # jsonRDD = sc.parallelize(json_str)
                            # # load it as DataFrame
                            # df = spark.read.json(jsonRDD)
                            # # write it out 
                            # df.write.parquet(f"{member.name}.parquet")
            break
    elapsed = time() - start
    print(f"Took {elapsed:g} sec") 
#%%
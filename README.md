# Extract Transform Load for Text Reuse data

A streaming approach to extract the contents of the textreuse data. 

## Prerequisites 

The following will be necessary to run the code

### Access to Allas

The zip files will be stored in a s3 bucket on [Pouta](pouta.csc.fi). Follow the [docs](https://docs.csc.fi/data/Allas/using_allas/s3_client/) to configure allas to obtain the s3 credentials. Then in the root directory of this project create a file `s3credentials.toml` with the following format:

```toml
[default]
aws_access_key_id=<access_key>
aws_secret_access_key=<secret_key>
endpoint_url=<endpoint_url>
```
This file will not be version controlled and will be used by `boto3` and `spark` to access the s3 bucktets to read and write data.

### Spark Cluster

There is a spark cluster set up on [Rahti](https://docs.csc.fi/apps/spark/). There are three types of pods that are always present namely, a master, notebook and worker. The master pod has a route to spark UI used to monitor the applications and workers. The notebook pod hosts a driver instance which has jupyter lab installed and attached to a permanent storage volume that is accessible to all the pods in the cluster. The worker pods are are the computational nodes and can be spun up (and down) on demand.

Follow the steps in the [docs](https://docs.csc.fi/cloud/rahti/usage/cli/#the-command-line-tools-page-in-the-openshift-web-ui) to download and install the openshift command line tools on your local machine and use the Rahti web interface to get the login command. Once the command line tools are up and running you can find the spark notebook pod as follows:
```bash
oc get pods | grep notebook
```
Then you can get a remote shell to the notebook pod using:
```bash
oc rsh <notebook-pod-name> bash
```
Now from inside the notebook pod this repository should be cloned. 
**Note**: The repository can also be run on a local spark cluster for development purposes/


**Note**: The exisitng spark cluster has a `spark-conf` set up in the Rahti config maps. If setting up a new cluster, ensure to make (or update) the spark configs.

## Python

Once the repository has been cloned install [poetry](https://python-poetry.org) for python dependency management. Create the virual env and ensure it is in the project directoy by running 
```bash
poetry config virtualenvs.in-project true
poetry install
```
This will install the python library required for the application.


## Zip to Parquet

### Format 

A rough structure of the data in the text reuse zip is as follows:
```
txtreuse.zip
|-iter_0_out.tar.gz
|	|-iter_0.json
|-iter_1_out.tar.gz
|	|-iter_0.json
|
```

### Streaming S3 zip files 

We use the [`smart_open`](https://github.com/RaRe-Technologies/smart_open) library to stream s3 obejcts in an efficient manner. While `smart_open` allows the [reading and writing of zipfiles](https://github.com/RaRe-Technologies/smart_open), there is a known performance issue in the python `zipfile` module which performs multiple to seeks to current positions of a file stream which causes a s3 buffer to be cleared as seen in this [issue](https://github.com/RaRe-Technologies/smart_open/pull/748). AS of writing this PR was not accepted in to the stable branch of the library. Therefore, we have [forked](https://github.com/HPC-HD/smart_open/tree/s3_ignore_seeks_to_current_position) and applied the patches suggested. We include this forked repo in the poetry dependency at the moment.


## Script 

Run the end-to-end etl job as follows on the notebook pod

```bash
spark-submit zip2parquet.py txtreuse.zip -v
```
You can monitor the progess on the spark master. 


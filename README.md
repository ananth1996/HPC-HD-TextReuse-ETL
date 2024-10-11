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
This file will not be version controlled and will be used by `boto3` and `spark` to access the s3 buckets to read and write data.

### Spark Cluster

There is a spark cluster set up on [Rahti](https://docs.csc.fi/apps/spark/). There are four types of pods that are always present namely, a master, notebook, a history server and worker. The master pod has a route to Spark UI used to monitor the applications and workers. The notebook pod hosts a driver instance which has Jupyter Lab installed and attached to a persistent storage volume that is accessible to all the pods in the cluster. The worker pods are the computational nodes and can be spun up (and down) on demand. The history server keeps track of all the applications run on the cluster and can be used after the application have finished running.

#### Setting Cluster on Rahti 

**Note**: The existing spark cluster has a `spark-conf` set up in the Rahti config maps. If setting up a new cluster, ensure to make (or update) the spark configs.

If the cluster needs to be set up from scratch, follow the instructions from the `all-spark` repository located at [https://github.com/HPC-HD/all-spark](https://github.com/HPC-HD/all-spark). The repository has Helm charts for the Spark clusters and values for setting up the spark cluster system.

#### Connecting to the Notebook Remotely

Follow the steps in the [docs](https://docs.csc.fi/cloud/rahti/usage/cli/#the-command-line-tools-page-in-the-openshift-web-ui) to download and install the openshift command line tools on your local machine and use the Rahti web interface to get the login command. Once the command line tools are up and running you can find the spark notebook pod as follows:

```bash
oc get pods | grep notebook
```

Then you can get a remote shell to the notebook pod using:

```bash
oc rsh <notebook-pod-name> bash
```

Now from inside the notebook pod this repository should be cloned.

## Python

Once the repository has been cloned install the dependencies using [poetry](https://python-poetry.org). Create the virual env and ensure it is in the project directory by running the following commands

```bash
poetry config virtualenvs.in-project true
poetry install
```

This will install the python library required for the application.

## MariaDB Database

Create a MariaDB instance following the details in the repository: [https://github.com/HPC-HD/pouta-mariadb-terraform/tree/main](https://github.com/HPC-HD/pouta-mariadb-terraform/tree/main).


The MariaDB instance will be used for Dagster and to load the final downstream assets.
For Dagster create a database called `dagster` and create a username and password for the service.

## Dagster

Create a `.env` file in the main project directory with the following details:

```
DAGSTER_HOME=/home/jovyan/work/etl_textreuse/dagster_home
AWS_ACCESS_KEY_ID=<>
AWS_SECRET_KEY=<>
AWS_ENDPOINT_URL="a3s.fi"
```

Then create a `dagster_home` in the main project directory and inside the folder create a file called `dagster.yaml`.

When on the production Spark cluster on Rahti add the concurrency and database details to the `dagster_home/dagster.yaml` file:

```yaml
run_queue:
  max_concurrent_runs: 1


storage:
  mysql:
    mysql_url: "mysql+mysqlconnector://{username}:{urlquote(password)}@{hostname}:{port}/{db_name}?charset=utf8mb4&collation=utf8mb4_general_ci"
#     mysql_db:
#       username: <dagster database username>
#       password: <dagster database password>
#       hostname: <database hostname>
#       db_name: "dagster"
#       port: 3306
```


Start the Dagster daemon and webserver as follows:

```bash
dagster-webserver -h 0.0.0.0
dagster-daemon run
```

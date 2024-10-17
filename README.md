# Extract Transform Load for Text Reuse data

A streaming approach to extract the contents of the textreuse data. 

- [Extract Transform Load for Text Reuse data](#extract-transform-load-for-text-reuse-data)
  - [Prerequisites](#prerequisites)
    - [Access to Allas](#access-to-allas)
    - [Spark Cluster](#spark-cluster)
      - [Setting Cluster on Rahti](#setting-cluster-on-rahti)
      - [Connecting to the Notebook Remotely](#connecting-to-the-notebook-remotely)
    - [Code and Python Dependencies](#code-and-python-dependencies)
    - [MariaDB Database](#mariadb-database)
    - [Dagster](#dagster)
  - [Next Steps](#next-steps)

## Prerequisites 

The following setup is necessary to run the pipeline.


> [!TIP]
> All configurations go in a `.env` file. Take a look at the [`.env-template` ](./.env-template) file for a template of all the environment variables required to be configured

### Access to Allas

The zip files will be stored in a s3 bucket on [Pouta](pouta.csc.fi). Follow the [docs](https://docs.csc.fi/data/Allas/using_allas/s3_client/) to configure Allas to obtain the s3 credentials. Then in the `.env` file in the root directory of this project add the following environment variables:

```bash
AWS_ACCESS_KEY_ID=<>
AWS_SECRET_KEY=<>
AWS_ENDPOINT_URL=<>
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

### Code and Python Dependencies

Once the repository has been cloned install the dependencies using [poetry](https://python-poetry.org). Create the virual env and ensure it is in the project directory by running the following commands

```bash
poetry config virtualenvs.in-project true
poetry install
```

This will install the python library required for the application. Then to package the python code and virtual environment for the Spark workers, do the following:

```bash
pip install .
tar -czvf venv.tar.gz -C .venv .
```

This will create a zip file with all the `etl_textreuse` module code and the python interpreters required by the Spark workers.

**Note: This step will need to be redone everytime the code changes**

In the `.env` file add the location to the zipped venv and the location of the Python interpreter for the Spark workers as follows:

```bash
PYSPARK_PYTHON="./venv/bin/python"
VENV_ZIP_FILE="<PROJECT ROOT>/venv.tar.gz#venv"
```

### MariaDB Database

Create a MariaDB instance following the details in the repository: [https://github.com/HPC-HD/pouta-mariadb-terraform/tree/main](https://github.com/HPC-HD/pouta-mariadb-terraform/tree/main).


The MariaDB instance will be used for Dagster and to load the final downstream assets. Create a database in the MariaDB instance where the downstream assets from the ETL pipeline will be loaded. Then provide the details to connect to the database via the following environment variables in the `.env` file:

```bash

DB_HOST=<MariaDB HOST IP>
DB_PORT=<PORT NUMBER>
DB_DRIVER="org.mariadb.jdbc.Driver"
DB_USERNAME=<USERNAME>
DB_PASSWORD=<PASSWORD>
DB_FETCHSISE="100000"
DB_BATCHSIZE="100000"
DB_DATABASE=<DATABASE NAME>
```


Additionally for Dagster, create a database, username and password. For example, run the following on the database with admin rights to create a `dagster_test` database with a `dagster_test_user` username :

```sql
create database dagster_test;
grant all privileges on dagster_test.* TO 'dagster_test_user'@'%' identified by '<password>';
flush privileges;
```

These will be used to create the `DAGSTER_MYSQL_DB_CONN_STRING` env variable.


### Dagster

Create a `.env` file in the main project directory with the following details:

```bash
DAGSTER_HOME=<PROJECT HOME>
DAGSTER_MYSQL_DB_CONN_STRING="mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{db_name}?charset=utf8mb4&collation=utf8mb4_general_ci"
```

Then create a `dagster_home` in the main project directory and inside the folder create a file called `dagster.yaml`.

When on the production Spark cluster on Rahti add the concurrency and database details to the `dagster_home/dagster.yaml` file:

```yaml
run_queue:
  max_concurrent_runs: 1


storage:
  mysql:
    mysql_url: 
      env: DAGSTER_MYSQL_DB_CONN_STRING
```


Start the Dagster daemon and webserver as follows:

```bash
dagster-webserver -h 0.0.0.0
dagster-daemon run
```

Then follow the route in Rahti to access the Dagster web interface.

## Next Steps

Take a look at [./etl_textreuse/README.md](/etl_textreuse/README.md) for details on how to use Dagster to materialize assets.

Check out [./etc_textreuse/assets/README.md](./etl_textreuse/assets/README.md) for comprehensive details about the various ETL assets. 
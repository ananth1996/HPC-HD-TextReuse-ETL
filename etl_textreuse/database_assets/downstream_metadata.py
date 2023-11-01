from etl_textreuse.spark_utils import get_spark_session,processed_bucket,get_s3,jdbc_opts
from etl_textreuse.database_utils import get_sqlalchemy_engine
from sqlalchemy import text
from dagster import asset


@asset(
    deps=["manifestation_publication_date"],
    group_name="database"
)
def db_manifestation_publication_date() -> None:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "manifestation_publication_date"
    database = "hpc-hd-newspapers"
    schema = """
    CREATE TABLE IF NOT EXISTS `manifestation_publication_date`(
        `manifestation_id_i` int(11) unsigned NOT NULL,
        `publication_date` date DEFAULT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"""
    index = """
    ALTER TABLE `manifestation_publication_date`
    ADD PRIMARY KEY (`manifestation_id_i`);
    """
    load_table(spark,table,processed_bucket,database,schema,index)



        

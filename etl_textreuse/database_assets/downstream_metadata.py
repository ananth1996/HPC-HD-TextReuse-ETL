from etl_textreuse.spark_utils import get_spark_session,load_table,processed_bucket
from etl_textreuse.database_utils import get_sqlalchemy_engine
from sqlalchemy import text
from dagster import asset, Output, MetadataValue


@asset(
    deps=["manifestation_publication_date"],
    group_name="database"
)
def db_manifestation_publication_date() -> Output[None]:
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
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)


@asset(
    deps=["manifestation_title"],
    group_name="database"
)
def db_manifestation_title() -> None:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "manifestation_title"
    database = "hpc-hd-newspapers"
    schema = """
    CREATE TABLE IF NOT EXISTS `manifestation_title`(
        `manifestation_id_i` int(11) unsigned NOT NULL,
        `title` text DEFAULT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"""
    index = """
    ALTER TABLE `manifestation_title`
    ADD PRIMARY KEY (`manifestation_id_i`);
    """
    load_table(spark,table,processed_bucket,database,schema,index)


        

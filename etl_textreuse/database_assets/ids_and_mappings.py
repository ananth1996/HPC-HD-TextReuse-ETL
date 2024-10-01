from etl_textreuse.spark_utils import get_spark_session,load_table,processed_bucket
from etl_textreuse.database_utils import get_sqlalchemy_engine
from sqlalchemy import text
from dagster import asset


@asset(
    deps=["textreuse_manifestation_mapping"],
    group_name="database"
)
def db_textreuse_manifestation_mapping() -> None:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "textreuse_manifestation_mapping"
    database = "hpc-hd-newspapers"
    schema = """
    CREATE TABLE IF NOT EXISTS `textreuse_manifestation_mapping`(
        `trs_id` int(11) unsigned NOT NULL,
        `manifestation_id_i` int(11) unsigned NOT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"""
    index = """
    ALTER TABLE `textreuse_manifestation_mapping`
        ADD INDEX IF NOT EXISTS `trs_id` (`trs_id`),
        ADD INDEX IF NOT EXISTS `manifestation_id_i` (`manifestation_id_i`);
    """
    load_table(spark,table,processed_bucket,database,schema,index)
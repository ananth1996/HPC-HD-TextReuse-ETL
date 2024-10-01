from etl_textreuse.spark_utils import get_spark_session,load_table,processed_bucket
from sqlalchemy import text
from dagster import asset,Output,MetadataValue

@asset(
    deps=["reception_edges_between_books_denorm"],
    group_name="database"
)
def db_reception_edges_between_books_denorm() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "reception_edges_between_books_denorm"
    database = "hpc-hd-newspapers"
    schema = """
    CREATE TABLE IF NOT EXISTS `reception_edges_between_books_denorm`(
        `src_trs_id` int(11) unsigned NOT NULL,
        `src_trs_start` int(11) unsigned NOT NULL,
        `src_trs_end` int(11) unsigned NOT NULL,
        `dst_trs_id` int(11) unsigned NOT NULL,
        `dst_trs_start` int(11) unsigned NOT NULL,
        `dst_trs_end` int(11) unsigned NOT NULL
    ) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """
    index = """
    ALTER TABLE `reception_edges_between_books_denorm`
    ADD INDEX IF NOT EXISTS `src_trs_id` (`src_trs_id`),
    ADD INDEX IF NOT EXISTS `dst_trs_id` (`dst_trs_id`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    
    return Output(None,metadata=metadata)

@asset(
    deps=["non_source_pieces"],
    group_name="database"
)
def db_non_source_pieces() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "non_source_pieces"
    database = "hpc-hd-newspapers"
    schema = """
    CREATE TABLE IF NOT EXISTS `non_source_pieces` (
        `cluster_id` int(11) unsigned NOT NULL,
        `piece_id` bigint(20) unsigned NOT NULL)
    ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """
    index = """
    ALTER TABLE `non_source_pieces` 
    ADD UNIQUE KEY `cluster_covering` (`cluster_id`,`piece_id`),
    ADD UNIQUE KEY `piece_covering` (`piece_id`,`cluster_id`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    
    return Output(None,metadata=metadata)
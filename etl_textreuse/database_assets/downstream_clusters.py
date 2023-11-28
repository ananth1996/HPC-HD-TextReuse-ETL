#%%
from etl_textreuse.spark_utils import get_spark_session,load_table,processed_bucket
from sqlalchemy import text
from dagster import asset,Output,MetadataValue
#%%

@asset(
    deps=["earliest_manifestation_and_pieces_by_cluster"],
    group_name="database"
)
def db_earliest_manifestation_and_pieces_by_cluster() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "earliest_manifestation_and_pieces_by_cluster"
    database = "hpc-hd-newspapers"
    schema = """
    CREATE TABLE IF NOT EXISTS `earliest_manifestation_and_pieces_by_cluster`(
        `cluster_id` int(11) unsigned NOT NULL,
        `manifestation_id_i` int(11) unsigned NOT NULL,
        `piece_id` bigint(20) unsigned NOT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"""
    index = """
    ALTER TABLE `earliest_manifestation_and_pieces_by_cluster`
    ADD INDEX IF NOT EXISTS `cluster_id` (`cluster_id`),
    ADD INDEX IF NOT EXISTS `manifestation_id_i` (`manifestation_id_i`),
    ADD INDEX IF NOT EXISTS `piece_id` (`piece_id`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    
    return Output(None,metadata=metadata)

@asset(
    deps=["earliest_book_and_pieces_by_cluster"],
    group_name="database"
)
def db_earliest_book_and_pieces_by_cluster() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "earliest_book_and_pieces_by_cluster"
    database = "hpc-hd-newspapers"
    schema = """
    CREATE TABLE IF NOT EXISTS `earliest_book_and_pieces_by_cluster`(
        `cluster_id` int(11) unsigned NOT NULL,
        `manifestation_id_i` int(11) unsigned NOT NULL,
        `piece_id` bigint(20) unsigned NOT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"""
    index = """
    ALTER TABLE `earliest_book_and_pieces_by_cluster`
    ADD INDEX IF NOT EXISTS `cluster_id` (`cluster_id`),
    ADD INDEX IF NOT EXISTS `manifestation_id_i` (`manifestation_id_i`),
    ADD INDEX IF NOT EXISTS `piece_id` (`piece_id`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    
    return Output(None,metadata=metadata)
#%%
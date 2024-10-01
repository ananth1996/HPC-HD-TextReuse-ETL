from etl_textreuse.spark_utils import get_spark_session,load_table,processed_bucket
from sqlalchemy import text
from dagster import asset,Output,MetadataValue

@asset(
    deps=["reception_inception_between_book_coverages"],
    group_name="database"
)
def db_reception_inception_between_book_coverages() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "reception_inception_between_book_coverages"
    database = "hpc-hd-newspapers"
    schema = """
    CREATE TABLE IF NOT EXISTS `reception_inception_between_book_coverages` (
        `src_trs_id` int(11) unsigned NOT NULL,
        `num_reuses_src` int(11) unsigned DEFAULT NULL,
        `reuses_src_in_dst` int(11) unsigned DEFAULT NULL,
        `src_length` int(11) unsigned DEFAULT NULL,
        `coverage_src_in_dst` double unsigned DEFAULT NULL,
        `dst_trs_id` int(11) unsigned NOT NULL,
        `num_reuses_dst` int(11) unsigned DEFAULT NULL,
        `reuses_dst_in_src` int(11) unsigned DEFAULT NULL,
        `dst_length` int(11) unsigned DEFAULT NULL,
        `coverage_dst_in_src` double unsigned DEFAULT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """
    index = """
    ALTER TABLE `reception_inception_between_book_coverages`
        ADD INDEX IF NOT EXISTS `src_trs_id` (`src_trs_id`),
        ADD INDEX IF NOT EXISTS `reception` (`coverage_src_in_dst`),
        ADD INDEX IF NOT EXISTS `dst_trs_id` (`dst_trs_id`),
        ADD INDEX IF NOT EXISTS `inception` (`coverage_dst_in_src`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    
    return Output(None,metadata=metadata)

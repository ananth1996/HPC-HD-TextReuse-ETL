from etl_textreuse.spark_utils import get_spark_session,load_table,processed_bucket
from sqlalchemy import text
from dagster import asset,Output,MetadataValue
import os 

@asset(
    deps=["reception_inception_coverages"],
    group_name="database"
)
def db_reception_inception_coverages() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "reception_inception_coverages"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `reception_inception_coverages` (
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
    ALTER TABLE `reception_inception_coverages`
        ADD INDEX IF NOT EXISTS `src_trs_id` (`src_trs_id`),
        ADD INDEX IF NOT EXISTS `reception` (`coverage_src_in_dst`),
        ADD INDEX IF NOT EXISTS `dst_trs_id` (`dst_trs_id`),
        ADD INDEX IF NOT EXISTS `inception` (`coverage_dst_in_src`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    
    return Output(None,metadata=metadata)

@asset(
    deps=["coverages"],
    group_name="database"
)
def db_coverages() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "coverages"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `coverages` (
        `trs1_id` int(11) unsigned NOT NULL,
        `t1_reuses` int(11) unsigned DEFAULT NULL,
        `reuse_t1_t2` int(11) unsigned DEFAULT NULL,
        `t1_length` int(11) unsigned DEFAULT NULL,
        `coverage_t1_t2` double unsigned DEFAULT NULL,
        `trs2_id` int(11) unsigned NOT NULL,
        `t2_reuses` int(11) unsigned DEFAULT NULL,
        `reuse_t2_t1` int(11) unsigned DEFAULT NULL,
        `t2_length` int(11) unsigned DEFAULT NULL,
        `coverage_t2_t1` double unsigned DEFAULT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """
    index = """
    ALTER TABLE `coverages`
        ADD INDEX IF NOT EXISTS `trs1_id` (`trs1_id`),
        ADD INDEX IF NOT EXISTS `coverage_t1_t2` (`coverage_t1_t2`),
        ADD INDEX IF NOT EXISTS `trs2_id` (`trs2_id`),
        ADD INDEX IF NOT EXISTS `coverage_t2_t1` (`coverage_t2_t1`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    
    return Output(None,metadata=metadata)

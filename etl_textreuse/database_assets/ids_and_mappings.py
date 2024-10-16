from etl_textreuse.spark_utils import get_spark_session,load_table,processed_bucket
from etl_textreuse.database_utils import get_sqlalchemy_engine
import os
from dagster import asset,Output

@asset(
    deps=["textreuse_ids"],
    group_name="database"
)
def db_textreuse_ids() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "textreuse_ids"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `textreuse_ids` (
        `trs_id` int(11) unsigned NOT NULL,
        `text_name` varchar(100),
        `manifestation_id` varchar(100),
        `structure_name` varchar(100)
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """ 
    index = """
    ALTER TABLE `textreuse_ids`
        ADD PRIMARY KEY (`trs_id`),
        ADD INDEX IF NOT EXISTS manifestation_trs_composite (`manifestation_id`,`trs_id`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["manifestation_ids"],
    group_name="database"
)
def db_manifestation_ids() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "manifestation_ids"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `manifestation_ids`(
        `manifestation_id_i` int(11) unsigned NOT NULL,
        `manifestation_id` varchar(100)
    ) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """ 
    index = """
    ALTER TABLE `manifestation_ids`
        ADD PRIMARY KEY (`manifestation_id_i`),
        ADD INDEX IF NOT EXISTS `manifestation_covering` (`manifestation_id`,`manifestation_id_i`); 
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["edition_ids"],
    group_name="database"
)
def db_edition_ids() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "edition_ids"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `edition_ids`(
        `edition_id_i` int(11) unsigned NOT NULL,
        `edition_id` varchar(100)
    ) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """ 
    index = """
    ALTER TABLE `edition_ids`
        ADD PRIMARY KEY (`edition_id_i`),
        ADD INDEX IF NOT EXISTS `edition_covering` (`edition_id`,`edition_id_i`); 
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["work_ids"],
    group_name="database"
)
def db_work_ids() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "work_ids"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `work_ids`(
        `work_id_i` int(11) unsigned NOT NULL,
        `work_id` varchar(2858)
    ) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """ 
    index = """
    ALTER TABLE `work_ids`
        ADD PRIMARY KEY (`work_id_i`),
        ADD INDEX IF NOT EXISTS `work_covering` (`work_id`,`work_id_i`); 
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["textreuse_manifestation_mapping"],
    group_name="database"
)
def db_textreuse_manifestation_mapping() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "textreuse_manifestation_mapping"
    database = os.getenv("DB_DATABASE")
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
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)


@asset(
    deps=["textreuse_work_mapping"],
    group_name="database"
)
def db_textreuse_work_mapping() -> None:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "textreuse_work_mapping"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `textreuse_work_mapping`(
        `trs_id` int(11) unsigned NOT NULL,
        `work_id_i` int(11) unsigned NOT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"""
    index = """
    ALTER TABLE `textreuse_work_mapping`
        ADD INDEX IF NOT EXISTS `trs_id` (`trs_id`),
        ADD INDEX IF NOT EXISTS `work_id_i` (`work_id_i`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["textreuse_edition_mapping"],
    group_name="database"
)
def db_textreuse_edition_mapping() -> None:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "textreuse_edition_mapping"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `textreuse_edition_mapping`(
        `trs_id` int(11) unsigned NOT NULL,
        `edition_id_i` int(11) unsigned NOT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"""
    index = """
    ALTER TABLE `textreuse_edition_mapping`
        ADD INDEX IF NOT EXISTS `trs_id` (`trs_id`),
        ADD INDEX IF NOT EXISTS `edition_id_i` (`edition_id_i`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["edition_mapping"],
    group_name="database"
)
def db_edition_mapping() -> None:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "edition_mapping"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `edition_mapping`(
        `manifestation_id_i` int(11) unsigned NOT NULL,
        `edition_id_i` int(11) unsigned NOT NULL
    ) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0; 
    """  
    index = """
    ALTER TABLE `edition_mapping`
        ADD INDEX IF NOT EXISTS `manifestation_id_i` (`manifestation_id_i`),
        ADD INDEX IF NOT EXISTS `edition_id_i` (`edition_id_i`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["work_mapping"],
    group_name="database"
)
def db_work_mapping() -> None:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "work_mapping"
    database = os.getenv("DB_DATABASE")
    schema = """
    CREATE TABLE IF NOT EXISTS `work_mapping`(
        `manifestation_id_i` int(11) unsigned NOT NULL,
        `work_id_i` int(11) unsigned NOT NULL
    ) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0; 
    """  
    index = """
    ALTER TABLE `work_mapping`
        ADD INDEX IF NOT EXISTS `manifestation_id_i` (`manifestation_id_i`),
        ADD INDEX IF NOT EXISTS `work_id_i` (`work_id_i`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)
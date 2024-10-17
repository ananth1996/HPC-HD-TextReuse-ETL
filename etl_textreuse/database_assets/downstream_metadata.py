from etl_textreuse.spark_utils import get_spark_session,load_table,processed_bucket
from etl_textreuse.database_utils import get_sqlalchemy_engine
from sqlalchemy import text
from dagster import asset, Output, MetadataValue
import os

@asset(
    deps=["manifestation_publication_date"],
    group_name="database"
)
def db_manifestation_publication_date() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "manifestation_publication_date"
    database = os.getenv('DB_DATABASE')
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
    deps=["edition_publication_date"],
    group_name="database"
)
def db_edition_publication_date() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "edition_publication_date"
    database = os.getenv('DB_DATABASE')
    schema = """
    CREATE TABLE IF NOT EXISTS `edition_publication_date`(
        `edition_id_i` int(11) unsigned NOT NULL,
        `publication_date` date DEFAULT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"""
    index = """
    ALTER TABLE `edition_publication_date`
        -- There might be editions with several possible publication dates
        ADD INDEX IF NOT EXISTS `edition_covering` (`edition_id_i`,`publication_date`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["work_earliest_publication_date"],
    group_name="database"
)
def db_work_earliest_publication_date() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "work_earliest_publication_date"
    database = os.getenv('DB_DATABASE')
    schema = """
    CREATE TABLE IF NOT EXISTS `work_earliest_publication_date` (
        `work_id_i` int(11) unsigned NOT NULL,
        `publication_date` date DEFAULT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """
    index = """
    ALTER TABLE `work_earliest_publication_date`
        ADD PRIMARY KEY (`work_id_i`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["manifestation_title"],
    group_name="database"
)
def db_manifestation_title() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "manifestation_title"
    database = os.getenv('DB_DATABASE')
    schema = """
    CREATE TABLE IF NOT EXISTS `manifestation_title`(
        `manifestation_id_i` int(11) unsigned NOT NULL,
        `title` text DEFAULT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"""
    index = """
    ALTER TABLE `manifestation_title`
    ADD PRIMARY KEY (`manifestation_id_i`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["actor_ids"],
    group_name="database"
)
def db_actor_ids() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "actor_ids"
    database = os.getenv('DB_DATABASE')
    schema = """
    CREATE TABLE IF NOT EXISTS `actor_ids`(
        `actor_id_i` int(11) unsigned NOT NULL,
        `actor_id` varchar(100),
        `name_unified` text
    ) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """
    index = """
    ALTER TABLE `actor_ids`
        ADD PRIMARY KEY (`actor_id_i`),
        ADD INDEX IF NOT EXISTS `actor_composite` (`actor_id`,`actor_id_i`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["edition_authors"],
    group_name="database"
)
def db_edition_authors() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "edition_authors"
    database = os.getenv('DB_DATABASE')
    schema = """
    CREATE TABLE IF NOT EXISTS `edition_authors` (
        `edition_id_i` int(11) unsigned NOT NULL,
        `actor_id_i` int(11) unsigned DEFAULT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """
    index = """
    ALTER TABLE `edition_authors`
        ADD INDEX IF NOT EXISTS `edition_id_i` (`edition_id_i`),
        ADD INDEX IF NOT EXISTS `actor_id_i` (`actor_id_i`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)

@asset(
    deps=["textreuse_source_lengths"],
    group_name="database"
)
def db_textreuse_source_lengths() -> Output[None]:
    spark = get_spark_session(application_name="Load MariaDB")
    table = "textreuse_source_lengths"
    database = os.getenv('DB_DATABASE')
    schema = """
    CREATE TABLE IF NOT EXISTS `textreuse_source_lengths` (
        `trs_id` int(11) unsigned NOT NULL,
        `text_length` int(11) unsigned DEFAULT NULL
    )ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
    """
    index = """
    ALTER TABLE `textreuse_source_lengths`
        ADD PRIMARY KEY (`trs_id`);
    """
    metadata = load_table(spark,table,processed_bucket,database,schema,index)
    return Output(None,metadata=metadata)
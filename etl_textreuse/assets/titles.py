from etl_textreuse.spark_utils import get_s3,get_spark_session,raw_bucket,processed_bucket,materialise_s3
from dagster import asset


@asset(
    deps=["ecco_core","eebo_core","newspapers_core","manifestation_ids"],
    description="The titles for each manifestation",
    group_name="downstream_metadata"
)
def manifestation_titles() -> None:
    spark = get_spark_session(application_name="manifestation titles")
    get_s3(spark,"ecco_core",raw_bucket)
    get_s3(spark,"eebo_core",raw_bucket)
    get_s3(spark,"newspapers_core",raw_bucket)
    get_s3(spark,"manifestation_ids",processed_bucket)
    materialise_s3(
        spark,
        fname="manifestation_titles",
        df = spark.sql(
        """
        SELECT manifestation_id_i,ecco_full_title as title FROM manifestation_ids mi 
        INNER JOIN ecco_core ec ON ec.ecco_id = mi.manifestation_id
        UNION ALL 
        -- because one eebo_tcp has multiple mappings 
        -- some of those can be NULL
        SELECT manifestation_id_i, max(eebo_tls_title) as title FROM manifestation_ids mi 
        INNER JOIN eebo_core ec ON ec.eebo_tcp_id = mi.manifestation_id
        GROUP BY manifestation_id_i
        UNION ALL 
        SELECT manifestation_id_i, newspaper_title as title  FROM manifestation_ids mi 
        INNER JOIN newspapers_core nc ON nc.article_id = mi.manifestation_id
        """),
        bucket=processed_bucket
    )
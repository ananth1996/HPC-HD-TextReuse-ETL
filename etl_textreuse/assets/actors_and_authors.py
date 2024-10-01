from etl_textreuse.spark_utils import *
from dagster import asset
from etl_textreuse.assets.upstream_metadata import estc_actors, estc_actor_links


@asset(
    deps=[estc_actors],
    description="Unique Actors and INT ids",
    group_name="downstream_metadata"
)
def actor_ids() -> None:
    spark = get_spark_session(project_root, "Actor IDs")
    get_s3(spark, "estc_actors", raw_bucket)
    materialise_row_numbers(
        spark,
        fname="actor_ids",
        df=spark.sql("""
        SELECT DISTINCT actor_id,name_unified
        FROM estc_actors
        ORDER BY actor_id,name_unified
        """),
        col_name="actor_id_i",
        bucket=processed_bucket
    )

# %%
# For every edition we have find the actors who are authors


@asset(
    deps=[actor_ids, "edition_ids", estc_actor_links],
    description="The actors for each edition",
    group_name="downstream_metadata"
)
def edition_authors() -> None:
    spark = get_spark_session(project_root, "Edition Authors")
    get_s3(spark, "estc_actor_links", raw_bucket)
    get_s3(spark, "actor_ids", processed_bucket)
    get_s3(spark, "edition_ids", processed_bucket)
    materialise_s3(
        spark,
        fname="edition_authors",
        df=spark.sql("""
        SELECT edition_id_i, actor_id_i
        FROM
        edition_ids ei
        LEFT JOIN estc_actor_links eal ON
        eal.estc_id = ei.edition_id 
        -- Ensure we only take actors who are authors
        AND eal.actor_role_author = 1
        LEFT JOIN actor_ids ai ON eal.actor_id = ai.actor_id
        """),
        bucket=processed_bucket

    )

# %%
from etl_textreuse.spark_utils import *
from dagster import asset, Output, MetadataValue
from etl_textreuse.assets.upstream_metadata import ecco_core, estc_core, eebo_core, newspapers_core
from etl_textreuse.assets.ids_and_mappings import manifestation_ids, textreuse_edition_mapping
# %%


@asset(
    deps=[estc_core, ecco_core, eebo_core, newspapers_core,
          manifestation_ids, "edition_mapping", "edition_ids"],
    description="The earliest publication dates of editions",
    group_name="downstream_metadata"
)
def edition_publication_date() -> Output[None]:
    spark = get_spark_session(
        project_root, application_name="Edition publication dates")
    get_s3(spark, "estc_core", raw_bucket)
    get_s3(spark, "eebo_core", raw_bucket)
    get_s3(spark, "ecco_core", raw_bucket)
    get_s3(spark, "newspapers_core", raw_bucket)
    get_s3(spark, "manifestation_ids", processed_bucket)
    get_s3(spark, "edition_ids", processed_bucket)
    get_s3(spark, "edition_mapping", processed_bucket)
    materialise_s3(
        spark,
        fname="edition_publication_date",
        df=spark.sql("""
        SELECT 
            em.edition_id_i,
            (CASE
                WHEN publication_year IS NULL THEN -- when estc_core doesn't have data
                (CASE 
                    WHEN LENGTH(eebo_tls_publication_date) = 4 THEN to_date(eebo_tls_publication_date,'yyyy') -- Eg: 1697
                    WHEN LENGTH(eebo_tls_publication_date) = 5 THEN to_date(SUBSTRING(eebo_tls_publication_date,-4),'yyyy') -- Eg: -1697
                    WHEN LENGTH(eebo_tls_publication_date) = 9 THEN to_date(SUBSTRING(eebo_tls_publication_date,1,4), 'yyyy') -- Eg: 1690-1697
                    WHEN LENGTH(eebo_tls_publication_date) > 9 THEN to_date(eebo_tls_publication_date,'LLLL d, yyyy') -- Eg: April 24, 1649
                END)
                ELSE to_date(CAST(estc.publication_year AS INT),'yyyy')
            END) AS publication_date
        FROM eebo_core ec
        INNER JOIN manifestation_ids mids ON ec.eebo_tcp_id = mids.manifestation_id
        INNER JOIN edition_mapping em USING(manifestation_id_i)
        INNER JOIN edition_ids eids USING(edition_id_i)
        LEFT JOIN estc_core estc ON eids.edition_id = estc.estc_id

        UNION

        SELECT edition_id_i,
            (CASE
                WHEN publication_year IS NULL AND ec.ecco_date_start != 0 -- when estc_core doesn't have data
                THEN to_date(SUBSTRING(CAST(ec.ecco_date_start AS INT),1,4),'yyyy') -- Eg: 1.7580101E7 -> 17580101 -> "1758" -> date(1758-01-01)
                WHEN publication_year IS NULL AND ec.ecco_date_start == 0 -- Don't record 0 years  
                THEN NULL
                ELSE to_date(CAST(estc.publication_year AS INT),'yyyy')
            END) AS publication_date
        FROM ecco_core ec
        INNER JOIN manifestation_ids mids ON ec.ecco_id = mids.manifestation_id
        INNER JOIN edition_mapping em USING(manifestation_id_i)
        INNER JOIN edition_ids eids USING(edition_id_i)
        LEFT JOIN estc_core estc ON eids.edition_id = estc.estc_id
        
        UNION 
                    
        SELECT edition_id_i, issue_start_date AS publication_date
        FROM newspapers_core nc
        INNER JOIN manifestation_ids mids ON nc.article_id = mids.manifestation_id
        INNER JOIN edition_mapping em USING(manifestation_id_i)
        """),
        bucket=processed_bucket
    )

    year_count_desc = (
        spark.sql("""
                  SELECT year(publication_date) as publication_year,COUNT(*) as count 
                  FROM edition_publication_date 
                  GROUP BY publication_year 
                  ORDER BY publication_year 
                  LIMIT 20""")
        .toPandas()
        .to_markdown()
    )

    return Output(None, metadata={"earliest years count": MetadataValue.md(year_count_desc)})


@asset(
    deps=[edition_publication_date, "edition_mapping", "work_mapping"],
    description="The earliest publication date of a work",
    group_name="downstream_metadata"
)
def work_earliest_publication_date() -> None:
    spark = get_spark_session(
        project_root, application_name="Earliest work publication date")
    get_s3(spark, "edition_publication_date", processed_bucket)
    get_s3(spark, "edition_mapping", processed_bucket)
    get_s3(spark, "work_mapping", processed_bucket)
    materialise_s3(
        spark,
        fname="work_earliest_publication_date",
        df=spark.sql("""
        SELECT work_id_i,MIN(publication_date) as publication_date FROM edition_publication_date
        LEFT JOIN edition_mapping USING(edition_id_i)
        LEFT JOIN work_mapping USING(manifestation_id_i)
        GROUP BY work_id_i
        """),
        bucket=processed_bucket
    )


@asset(
        deps=[textreuse_edition_mapping, edition_publication_date], 
        description="The earliest publication date of a textreuse source",
        group_name="textreuse_downstream_metadata_link"
)
def textreuse_earliest_publication_date() -> None:
    spark = get_spark_session(
        project_root, application_name="Earliest textreuse publication date")
    get_s3(spark, "edition_publication_date", processed_bucket)
    get_s3(spark, "textreuse_edition_mapping", processed_bucket)
    materialise_s3(
        spark,
        fname="textreuse_earliest_publication_date",
        df=spark.sql("""
        SELECT 
            trs_id, 
            MIN(publication_date) as publication_date
        FROM textreuse_edition_mapping 
        INNER JOIN edition_publication_date epy USING(edition_id_i)
        GROUP BY trs_id
        """),
        bucket=processed_bucket
    )

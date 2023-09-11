#%%
from etl_textreuse.spark_utils import *
from dagster import asset,Output, multi_asset, AssetOut
from etl_textreuse.assets.upstream_metadata import ecco_core,estc_core,eebo_core,newspapers_core
from etl_textreuse.assets.raw_textreuses import textreuse_ids
#%%

# Create INT ids for the manifestation ids which are 
#  ECCO and EEBO_TCP
@asset(deps=[ecco_core,eebo_core,newspapers_core],description="The text manifestations used in textreuses")
def manifestation_ids() -> Output[None]:
    spark = get_spark_session(project_root,application_name="manifestation_ids")
    get_s3(spark,"ecco_core",raw_bucket)
    get_s3(spark,"eebo_core",raw_bucket)
    get_s3(spark,"newspapers_core",raw_bucket)

    manifestation_ids = materialise_row_numbers(
        spark,
        fname="manifestation_ids",
        df=spark.sql("""
        SELECT * FROM(
            SELECT DISTINCT ecco_id AS manifestation_id FROM ecco_core
            UNION ALL 
            SELECT DISTINCT eebo_tcp_id AS manifestation_id FROM eebo_core 
            WHERE eebo_tcp_id IS NOT NULL
            UNION ALL 
            SELECT DISTINCT article_id AS manifestation_id FROM newspapers_core
        )
        ORDER BY manifestation_id
        """),
        col_name="manifestation_id_i",
        bucket=processed_bucket
    )
    count = manifestation_ids.count()
    return Output(None,metadata={"rows":count})



# create mapping between documents from manifestations and editions.
# For books this is bewtwen ECCO and EEBO_TCP to ESTC id.
# There are 1143 EEBO_TCP documents that don'y have a ESTC id, 
#       in those cases just use the EEBO_TCP id as a placeholder
#       call this an edition_id.
# Similarly, there are some ECCO documents that don't have an ESTC id,
#       in those cases the ECCO id is used as placeholder edition
# For newspapers there is not true edition. While there are issues, these
# are logically different. Therefore, we just map each article to its own individual issue
@multi_asset(
    outs={
        "edition_mapping":AssetOut(description="The mapping from manifestations to editions"),
        "edition_ids":AssetOut(description="The unique editions and their ids")
    },
    deps=[ecco_core,eebo_core,newspapers_core,manifestation_ids]
)
def edition_ids_and_mappings():
    spark = get_spark_session(project_root,application_name="edition ids and mappings")
    get_s3(spark,"ecco_core",raw_bucket)
    get_s3(spark,"eebo_core",raw_bucket)
    get_s3(spark,"newspapers_core",raw_bucket)
    get_s3(spark,"manifestation_ids",processed_bucket)

    materialise_with_int_id(
        spark,
        fname="edition_mapping",
        df=spark.sql("""
        SELECT DISTINCT
            manifestation_id_i,
            estc_id AS edition_id
        FROM ecco_core ecco
        INNER JOIN manifestation_ids mids ON mids.manifestation_id = ecco.ecco_id

        UNION ALL

        SELECT DISTINCT
            manifestation_id_i,
            (CASE 
                WHEN estc_id iS NULL THEN eebo_tcp_id
                ELSE estc_id
            END) AS edition_id
        FROM eebo_core eebo
        INNER JOIN manifestation_ids mids ON mids.manifestation_id = eebo.eebo_tcp_id
                    
        UNION ALL 
                    
        SELECT 
            manifestation_id_i,
            article_id AS edition_id 
        FROM newspapers_core news
        INNER JOIN manifestation_ids mids ON mids.manifestation_id = news.article_id
        """),
        col_name="edition_id",
        id_col_name="edition_id_i",
        keep_id_mapping=True,
        id_fname="edition_ids",
        bucket=processed_bucket,
        drop_col=True
    )
    edition_ids = get_s3(spark,"edition_ids",processed_bucket)
    editions_count = edition_ids.count()
    return Output(None),Output(None,metadata={"rows":editions_count})



# For each edition find the work_id from ESTC
#  and if the information is not present in ESTC (as for 113 ECCO documents)
#  or if the edition_id is new (the EEBO_TCP documents from above),
#  then make new work ids with the same manifestation_id
# This will the default for the newspapers data which has no mapping to ESTC
@multi_asset(
    outs={
        "work_mapping":AssetOut(description="The mapping from manifestations to works"),
        "work_ids":AssetOut(description="The unique works and their ids")
    },
    deps=[estc_core,newspapers_core,manifestation_ids,"edition_ids","edition_mapping"]
)
def work_ids_and_mapping():
    spark = get_spark_session(project_root,application_name="work ids and mapping")
    get_s3(spark,"estc_core",raw_bucket)
    get_s3(spark,"edition_mapping",processed_bucket)
    get_s3(spark,"manifestation_ids",processed_bucket)
    get_s3(spark,"edition_ids",processed_bucket)
    materialise_with_int_id(
        spark,
        fname = "work_mapping",
        df=spark.sql("""
        SELECT DISTINCT
            em.manifestation_id_i,
            (CASE
                WHEN ec.work_id IS NULL THEN mids.manifestation_id
                ELSE ec.work_id
            END) AS work_id
        FROM edition_mapping em
        INNER JOIN manifestation_ids mids USING (manifestation_id_i)
        INNER JOIN edition_ids eids USING (edition_id_i)
        LEFT JOIN estc_core ec ON eids.edition_id = ec.estc_id
        """),
        col_name="work_id",
        id_col_name="work_id_i",
        keep_id_mapping=True,
        id_fname="work_ids",
        bucket=processed_bucket,
        drop_col=True
    )
    work_ids = get_s3(spark,"work_ids",processed_bucket)
    works_count = work_ids.count()
    return Output(None),Output(None,metadata={"rows":works_count})


@asset(deps=[textreuse_ids,"edition_mapping",manifestation_ids],description="Mapping between textreuses and editions")
def textreuse_edition_mapping() -> None:        
    spark = get_spark_session(project_root,application_name="textreuse edition mapping")
    get_s3(spark,"textreuse_ids",bucket=processed_bucket)
    get_s3(spark,"manifestation_ids",bucket=processed_bucket)
    get_s3(spark,"edition_mapping",bucket=processed_bucket)
    materialise_s3(
        spark,
        fname="textreuse_edition_mapping",
        df=spark.sql("""
        SELECT DISTINCT trs_id, edition_id_i
        FROM textreuse_ids ti
        INNER JOIN manifestation_ids mids USING(manifestation_id)
        INNER JOIN edition_mapping em USING(manifestation_id_i) 
        """),
        bucket=processed_bucket
    )

@asset(deps=[textreuse_ids,manifestation_ids,"work_mapping"],description="Mapping between textreuses and works")
def textreuse_work_mapping() -> None:
    spark = get_spark_session(project_root,application_name="textreuse work mapping")
    get_s3(spark,"textreuse_ids",bucket=processed_bucket)
    get_s3(spark,"manifestation_ids",bucket=processed_bucket)
    get_s3(spark,"work_mapping",bucket=processed_bucket)
    materialise_s3(
        spark,
        fname="textreuse_work_mapping",
        df=spark.sql("""
        SELECT DISTINCT trs_id,work_id_i
        FROM textreuse_ids ti
        INNER JOIN manifestation_ids USING (manifestation_id)
        INNER JOIN work_mapping wm USING (manifestation_id_i)
        """),
        bucket=processed_bucket
    )


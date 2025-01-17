#%%
from ..spark_utils import *
from dagster import asset,AssetKey, AssetSpec
#%%

ecco_core = AssetSpec(key=AssetKey("ecco_core"),description="ECCO Core metadata",group_name="metadata")
eebo_core = AssetSpec(key=AssetKey("eebo_core"),description="EEBO Core metadata",group_name="metadata")
estc_core = AssetSpec(key=AssetKey("estc_core"),description="ESTC Core metadata",group_name="metadata")
estc_actors = AssetSpec(key=AssetKey("estc_actors"),description="Actros in the ESTC metadata",group_name="metadata")
estc_actor_links = AssetSpec(key=AssetKey("estc_actor_links"),description="The links between ESTC and Actors",group_name="metadata")
newspapers_raw = AssetSpec(key=AssetKey("bl_newspapers_meta"),description="Raw newspapres metadata",group_name="metadata")
@asset(
        deps=[newspapers_raw],
        description="Newspapers Core Metadata",
        group_name="metadata"
)
def newspapers_core() -> None:
    newspapers_raw_csv = f"s3a://{raw_bucket}/bl_newspapers_meta.csv"
    spark = get_spark_session(project_root,application_name="Newspapers Core")
    newspapers_core = spark.read.option("header",True).csv(newspapers_raw_csv)
    newspapers_core.createOrReplaceTempView("newspapers_core")
    materialise_s3(
        spark,
        fname="newspapers_core",
        df=spark.sql("""
        SELECT *,
        (CASE 
        WHEN to_date(issue_date_start,'yyyy-MM-dd') IS NULL -- when str is '1732-00-00'
        THEN to_date(SUBSTRING(issue_date_start,1,4),'yyyy') -- '1732-00-00' -> '1732' -> 1732-01-01
        ELSE to_date(issue_date_start,'yyyy-MM-dd')
        END) AS issue_start_date,
        (CASE 
        WHEN to_date(issue_date_end,'yyyy-MM-dd') IS NULL 
        THEN to_date(SUBSTRING(issue_date_end,1,4),'yyyy')
        ELSE to_date(issue_date_end,'yyyy-MM-dd')
        END) AS issue_end_date
        FROM newspapers_core
        """),
        bucket=raw_bucket
    )

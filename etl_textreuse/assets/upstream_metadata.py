#%%
from etl_textreuse.assets.orig_textreuses import orig_pieces, orig_textreuses
from etl_textreuse.spark_utils import *
from dagster import asset,AssetKey, SourceAsset
#%%

ecco_core = SourceAsset(key=AssetKey("ecco_core"),description="ECCO Core metadata")
eebo_core = SourceAsset(key=AssetKey("eebo_core"),description="EEBO Core metadata")
estc_core = SourceAsset(key=AssetKey("estc_core"),description="ESTC Core metadata")
newspapers_raw = SourceAsset(key=AssetKey("bl_newspapers_meta"),description="Raw newspapres metadata")

@asset(deps=[newspapers_raw],description="Newspapers Core Metadata")
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

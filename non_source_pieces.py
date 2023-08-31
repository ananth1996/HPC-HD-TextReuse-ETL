
#%%
from IPython import get_ipython
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path
from spark_utils import *
from db_utils import *
from pyspark.sql.functions import col
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
import numpy as np
from sqlalchemy import text
import toml
#%%


if __name__ == "__main__":

    earliest_work_and_pieces_by_cluster = get_s3("earliest_work_and_pieces_by_cluster",processed_bucket)
    clustered_defrag_pieces = get_s3("clustered_defrag_pieces",processed_bucket)
    non_source_pieces = materialise_s3_if_not_exists(
        fname="non_source_pieces",
        df = spark.sql("""
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
        """),
        bucket=processed_bucket
    )

    schema = "CREATE TABLE IF NOT EXISTS `non_source_pieces` (`cluster_id` int(11) unsigned NOT NULL,`piece_id` bigint(20) unsigned NOT NULL)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"
    indexes = "ALTER TABLE `non_source_pieces` ADD UNIQUE KEY `cluster_covering` (`cluster_id`,`piece_id`),ADD UNIQUE KEY `piece_covering` (`piece_id`,`cluster_id`);"

    conn = get_sqlalchemy_connect(version="mariadbNewspapers")
    # Remove any table that exists
    conn.execute(text("DROP TABLE IF EXISTS `non_source_pieces`;"))
    conn.execute(text(schema))
    print("Schema Created")
    (
        jdbc_opts(non_source_pieces.write)
        .option("dbtable", "non_source_pieces") 
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    print("Table Loaded")
    conn.execute(text(indexes))
    print("Indexes Created")
    conn.close()
    print("Finishing script")




# %%

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
from pyspark.sql.functions import col
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
import numpy as np
from sqlalchemy import text
import toml
#%%
earliest_work_and_pieces_by_cluster = get_s3("earliest_work_and_pieces_by_cluster",processed_bucket)
clustered_defrag_pieces = get_s3("clustered_defrag_pieces",processed_bucket)

if (project_root/"non_source_pieces").exists:
    print("Data exists loading")
    non_source_pieces = get_local("non_source_pieces")
else:
    print("Data doesn't exists creating")
    non_source_pieces = materialise_local(
        name="non_source_pieces",
        df = spark.sql("""
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
        """),
        cache=False
    )
#%%

schema = "CREATE TABLE IF NOT EXISTS `non_source_pieces` (`cluster_id` int(11) unsigned NOT NULL,`piece_id` bigint(20) unsigned NOT NULL)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;"

indexes = "ALTER TABLE `non_source_pieces` ADD UNIQUE KEY `cluster_covering` (`cluster_id`,`piece_id`),ADD UNIQUE KEY `piece_covering` (`piece_id`,`cluster_id`);"

conn = get_sqlalchemy_connection()
conn.execute(text("DROP TABLE IF EXISTS `non_source_pieces`;"))
conn.execute(text(schema))
(
    jdbc_opts(non_source_pieces.write)
    .option("dbtable", "non_source_pieces") 
    .option("truncate", "true")
    .mode("overwrite")
    .save()
)
conn.execute(text(indexes))
conn.close()





# %%

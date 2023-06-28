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
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
#%%[markdown]
# Load the metadata tables
metadata_tables = [
    ["textreuse_ids",processed_bucket,],
    ["manifestation_ids",processed_bucket,],
    ["edition_ids",processed_bucket,],
    ["work_ids",processed_bucket,],
    ["actor_ids",processed_bucket,],
    ["textreuse_work_mapping",processed_bucket,],
    ["textreuse_edition_mapping",processed_bucket,],
    ["work_mapping",processed_bucket,],
    ["edition_mapping",processed_bucket,],
    ["edition_publication_year",processed_bucket,],
    ["work_earliest_publication_year",processed_bucket,],
    ["edition_authors",processed_bucket,],
    ["estc_core",raw_bucket,],
    ["ecco_core",raw_bucket,],
    ["eebo_core",raw_bucket,],
    ["eebo_tcp_core",raw_bucket,],
    ["estc_actor_links",raw_bucket,],
    ["estc_actors",raw_bucket,],
]
for table,bucket in metadata_tables:
    df =  get_s3(table,bucket)
    (
        jdbc_opts(df.write).
        option("createTableOptions","ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0")
        .option("dbtable", table) 
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
#%%[markdown]
# Load the data tables
#%%

data_tables = [
    ["defrag_pieces",processed_bucket,],
    ["defrag_textreuses",processed_bucket,],
    ["clustered_defrag_pieces",processed_bucket,],
    ["earliest_textreuse_by_cluster",processed_bucket,],
    ["earliest_work_and_pieces_by_cluster",processed_bucket,],
    ["reception_edges",processed_bucket,],
    ["source_piece_statistics",processed_bucket,],
    ["reception_edges_denorm",denorm_bucket,],
    ["source_piece_statistics_denorm",denorm_bucket,],
]
for table,bucket in data_tables:
    df =  get_s3(table,bucket)
    (
        jdbc_opts(df.write).
        option("createTableOptions","ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0")
        .option("dbtable", table) 
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

# %%
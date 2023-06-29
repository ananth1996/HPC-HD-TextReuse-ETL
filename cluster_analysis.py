#%%
from IPython import get_ipython
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path
from pathlib import Path
from spark_utils import *
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
from graphframes import *
#%%
defrag_pieces = get_s3("defrag_pieces",processed_bucket).withColumnRenamed("piece_id","id")
defrag_textreuses = get_s3("defrag_textreuses",processed_bucket).withColumnRenamed("piece1_id","src").withColumnRenamed("piece2_id","dst")
# %%
G = GraphFrame(defrag_pieces, defrag_textreuses)
#%%
checkpoint_dir = project_root/"checkpoints"
checkpoint_dir.mkdir(exist_ok=True,parents=True)
sc.setCheckpointDir(str(checkpoint_dir))
#%%
if not s3_uri_exists(f"s3a://{processed_bucket}/defrag_pieces_connected_components.parquet"):
    defrag_pieces_connected_components = materialise_s3(
        fname = "defrag_pieces_connected_components",
        df =G.connectedComponents().withColumnRenamed("id","piece_id").select("piece_id","component"),
        bucket= processed_bucket
    )
else:
    defrag_pieces_connected_components = get_s3("defrag_pieces_connected_components",processed_bucket)
#%%
clustered_defrag_pieces = get_s3("clustered_defrag_pieces",processed_bucket)
# %%


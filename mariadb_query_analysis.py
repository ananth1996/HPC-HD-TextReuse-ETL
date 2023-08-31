#%%
from IPython import get_ipython
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path
from pyspark.sql.functions import col
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
from db_utils import *
import numpy as np
import pandas as pd
from sqlalchemy import text
from tqdm.autonotebook import trange,tqdm
import re
import seaborn as sns
from plot_utils import log_binning
import matplotlib.pyplot as plt
#%%
denorm_query=text("""
SELECT COUNT(*) FROM reception_edges_denorm re 
INNER JOIN textreuse_ids ti 
ON re.src_trs_id = ti.trs_id WHERE ti.manifestation_id=:doc_id""")
                  
intermediate_query = text("""
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id=:doc_id
), 
doc_pieces AS (
    SELECT * FROM defrag_pieces 
    INNER JOIN doc_trs_ids USING(trs_id)
),
doc_clusters_and_src_pieces AS (
    SELECT 
        cluster_id,
        dp.*
    FROM earliest_work_and_pieces_by_cluster
    INNER JOIN doc_pieces dp USING(piece_id)
),final_result AS (
SELECT  
    dp1.trs_id AS src_trs_id,
    dp1.trs_start AS src_trs_start,
    dp1.trs_end AS src_trs_end,
    dp2.trs_id AS dst_trs_id,
    dp2.trs_start AS dst_trs_start,
    dp2.trs_end AS dst_trs_end
FROM 
doc_clusters_and_src_pieces dp1
INNER JOIN non_source_pieces nsp USING(cluster_id) 
INNER JOIN defrag_pieces dp2 ON nsp.piece_id = dp2.piece_id
)
SELECT COUNT(*) FROM final_result""")
                          
standard_query = text("""
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id=:doc_id
), 
non_source_pieces_tmp AS (
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
),
doc_pieces AS (
    SELECT * FROM defrag_pieces 
    INNER JOIN doc_trs_ids USING(trs_id)
),
doc_clusters_and_src_pieces AS (
    SELECT 
        cluster_id,
        dp.*
    FROM earliest_work_and_pieces_by_cluster
    INNER JOIN doc_pieces dp USING(piece_id)
),final_result AS (
SELECT  
    dp1.trs_id AS src_trs_id,
    dp1.trs_start AS src_trs_start,
    dp1.trs_end AS src_trs_end,
    dp2.trs_id AS dst_trs_id,
    dp2.trs_start AS dst_trs_start,
    dp2.trs_end AS dst_trs_end
FROM 
doc_clusters_and_src_pieces dp1
INNER JOIN non_source_pieces_tmp nsp USING(cluster_id) 
INNER JOIN defrag_pieces dp2 ON nsp.piece_id = dp2.piece_id
)
SELECT COUNT(*) FROM final_result""")
#%%
df = pd.read_csv("./data/num_reception_edges_newspapers.csv")
manifestation_ids = df.manifestation_id.values
num_reception_edges = df.num_reception_edges.values
# quantile = np.quantile(num_reception_edges,[0,0.25,0.5,0.75,1])
q = np.linspace(0,1,5)
_quantile = np.quantile(num_reception_edges,q)
seed = 2
num_samples = 100
_num_samples = int(num_samples/(len(q)-1))
rng = np.random.default_rng(seed=seed)
samples = {i:[] for i in range(len(q)-1)}
sample_results = {i:[] for i in range(len(q)-1)}
for i,(low,high) in enumerate(zip(_quantile[:-1],_quantile[1:])):
    quantile_mask = (num_reception_edges>=low)&(num_reception_edges<=high)
    mask_size = sum(quantile_mask)
    sample_ids = rng.choice(a=mask_size,size=_num_samples,replace=False)
    _samples = manifestation_ids[quantile_mask][sample_ids]
    _sample_results = num_reception_edges[quantile_mask][sample_ids]
    samples[i].extend(_samples)
    sample_results[i].extend(_sample_results)
    print(f"Quantile {i}: Sampling {_num_samples} from {low=:g} and {high=:g}")

samples_df = pd.DataFrame.from_dict(samples).melt(var_name="quantile",value_name="doc_id")
sample_results_df = pd.DataFrame.from_dict(sample_results).melt(var_name="quantile",value_name="ground_truth")
samples_df = samples_df.join(sample_results_df["ground_truth"])
print("Maximum Number of rows:",sample_results_df.ground_truth.max())
#%%
dfs = []
doc_id_pattern=re.compile("manifestation_id='(.*?)'")
for query,query_type in tqdm(zip([denorm_query,intermediate_query,standard_query],["denorm","intermediate","standard"]),total=3):   
    conn = get_sqlalchemy_connect(version="mariadbNewspapers")
    conn.execute(text("SET SESSION profiling=0;"))
    # conn.execute(text("FLUSH NO_WRITE_TO_BINLOG TABLES reception_edges_denorm,non_source_pieces,manifestation_ids,earliest_work_and_pieces_by_cluster,defrag_pieces,clustered_defrag_piecess;"))
    conn.execute(text("RESET QUERY CACHE;"))
    conn.execute(text("SET SESSION profiling=1;"))
    conn.execute(text("SET SESSION profiling_history_size=100;"))
    for quantile in trange(len(q)-1):
            for doc_id,ground_truth in tqdm(zip(samples[quantile],sample_results[quantile]),total=_num_samples):
                result = conn.execute(query,parameters=dict(doc_id=doc_id)).fetchall()[0][0]
                assert result == ground_truth

    profiles = conn.execute(text("SHOW PROFILES;")).fetchall()
    conn.close()
    results = pd.DataFrame(profiles)
    results["doc_id"]=results.Query.apply(lambda s: doc_id_pattern.search(s).group(1))
    results = results.merge(samples_df,on="doc_id")
    results  = results.drop(columns=["Query_ID","Query"])
    results["query_type"]=query_type
    dfs.append(results)
#%%
r_df = pd.concat(dfs)
bin_centres,hist = log_binning(num_reception_edges,density=False)
plt.loglog(bin_centres,hist)
plt.xlabel("Number of Reception Edges")
plt.ylabel("Number of documents")
plt.title("Power Law distribution")
#%%
sns.barplot(data=r_df,x="quantile",y='ground_truth',log=True)
plt.ylabel("Number of Reception Edges")
#%%
sns.barplot(data=r_df,x="quantile",y='Duration',hue="query_type",log=True)
plt.ylabel("Duration (in seconds)")
# %%

# %%

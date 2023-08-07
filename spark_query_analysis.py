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
    project_root = Path(__file__).parent.resolve()
from time import perf_counter as time
import numpy as np
import pandas as pd
from tqdm.autonotebook import trange,tqdm
#%%
denorm_query = """
SELECT COUNT(*) 
FROM 
reception_edges_denorm re
INNER JOIN textreuse_ids ti ON re.src_trs_id = ti.trs_id
WHERE ti.manifestation_id={doc_id!r}
"""
#%%
# run the same query on unmaterialized tables
standard_query = """
WITH non_source_pieces_tmp AS (
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster ewapbca2 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
    ),
doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id={doc_id!r}
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
), final_result AS (
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
SELECT COUNT(*) FROM final_result
"""

intermediate_query = """
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id={doc_id!r}
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
SELECT COUNT(*) FROM final_result"""
#%%
if __name__ == "__main__":

    data_dir = project_root/"data"
    if (file_loc:=data_dir/"num_reception_edges.csv").exists():
        print("Ground Truth file exists loading")
        df = pd.read_csv(file_loc)
    else:
        print("Ground Truth file not present. Creating...")
        textreuse_ids = get_s3("textreuse_ids",processed_bucket)
        reception_edges_denorm = get_s3("reception_edges_denorm",denorm_bucket)
        sdf = spark.sql("SELECT ti.manifestation_id, COUNT(*) as num_reception_edges FROM reception_edges_denorm INNER JOIN textreuse_ids ti ON src_trs_id = ti.trs_id  GROUP BY ti.manifestation_id")
        df = sdf.toPandas()
        df = df.sort_values(by=["num_reception_edges","manifestation_id"],ascending=[False,True])
        df.to_csv(file_loc,index=False)

    manifestation_ids = df.manifestation_id.values
    num_reception_edges = df.num_reception_edges.values
    q = np.linspace(0,1,5)
    _quantile = np.quantile(num_reception_edges,q)
    seed = 42
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
    #%%
    # load parquets
    print("Loading Parquets")
    earliest_work_and_pieces_by_cluster = get_s3("earliest_work_and_pieces_by_cluster",processed_bucket)
    clustered_defrag_pieces = get_s3("clustered_defrag_pieces",processed_bucket)
    defrag_pieces = get_s3("defrag_pieces",processed_bucket)
    textreuse_ids = get_s3("textreuse_ids",processed_bucket)
    reception_edges_denorm = get_s3("reception_edges_denorm",denorm_bucket)
    non_source_pieces = get_s3("non_source_pieces",processed_bucket)
    rows =[]
    for query,query_type in tqdm(zip([denorm_query,intermediate_query,standard_query],["denorm","intermediate","standard"]),total=3):            
        for quantile in trange(len(q)-1):
            for doc_id,ground_truth in tqdm(zip(samples[quantile],sample_results[quantile]),total=_num_samples):
                start = time()
                result = spark.sql(query.format(doc_id=doc_id)).first()[0]
                result_time = time() - start
                assert result == ground_truth
                row = {
                    "quantile":quantile,
                    "doc_id":doc_id,
                    "ground_truth":ground_truth,
                    "Duration":result_time,
                    "query_type":query_type
                }
                rows.append(row)

    results_df = pd.DataFrame(rows)
    results_df.to_csv("./data/spark_reception_query_results.csv",index=False)
#%%
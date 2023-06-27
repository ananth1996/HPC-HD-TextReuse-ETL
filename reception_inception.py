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
from pyspark.sql.functions import col
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
from time import perf_counter as time
import numpy as np
import pandas as pd
from tqdm.autonotebook import trange,tqdm
#%%
earliest_work_and_pieces_by_cluster = get_s3("earliest_work_and_pieces_by_cluster",processed_bucket)
clustered_defrag_pieces = get_s3("clustered_defrag_pieces",processed_bucket)
defrag_pieces = get_s3("defrag_pieces",processed_bucket)
textreuse_ids = get_s3("textreuse_ids",processed_bucket)
#%%
# materialize a downstream table specifically for the task
reception_edges = materialise_s3_if_not_exists(
    fname = "reception_edges",
    df = spark.sql("""
    WITH non_source_pieces AS (
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster ewapbca2 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
    )
    SELECT ewapbca.piece_id as src_piece_id, nsp.piece_id as dst_piece_id
    FROM earliest_work_and_pieces_by_cluster ewapbca 
    -- only if a cluster has non_source pieces add edges
    --  hence, some clusters which are only source pieces will not have edges
    INNER JOIN non_source_pieces nsp USING(cluster_id)
    """),
    bucket=processed_bucket,
)
#%%
## Create a denormalized version of the reception edges 
##   here the pieces are denormalizes to ensure quicker searches
reception_edges_denorm = materialise_s3_if_not_exists(
    fname="reception_edges_denorm",
    df = spark.sql("""
    SELECT 
        dp1.trs_id AS src_trs_id,
        dp1.trs_start AS src_trs_start,
        dp1.trs_end AS src_trs_end,
        dp2.trs_id AS dst_trs_id,
        dp2.trs_start AS dst_trs_start,
        dp2.trs_end AS dst_trs_end
    FROM 
    reception_edges re
    INNER JOIN defrag_pieces dp1 ON re.src_piece_id = dp1.piece_id
    INNER JOIN defrag_pieces dp2 ON re.dst_piece_id = dp2.piece_id
    """),
    bucket=denorm_bucket
)
#%%
denorm_query = """
SELECT re.* 
FROM 
reception_edges_denorm re
INNER JOIN textreuse_ids ti ON re.src_trs_id = ti.trs_id
WHERE ti.manifestation_id = {doc_id!r}
"""
#%%
# run the same query on unmaterialized tables
standard_query = """
WITH non_source_pieces AS (
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster ewapbca2 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
    ),
doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id = {doc_id!r}
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
)
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
"""
#%%
if __name__ == "__main__":
        
    df = spark.sql("SELECT ti.manifestation_id, COUNT(*) as num_reception_edges FROM reception_edges_denorm INNER JOIN textreuse_ids ti ON src_trs_id = ti.trs_id  GROUP BY ti.manifestation_id")
    pdf = df.toPandas()
    pdf = pdf.sort_values(by=["num_reception_edges","manifestation_id"],ascending=[False,True])
    manifestation_ids = pdf.manifestation_id.values
    num_reception_edges = pdf.num_reception_edges.values
    quartile = np.quantile(num_reception_edges,[0,0.25,0.5,0.75,1])
    seed = 42
    num_samples = 100
    _num_samples = int(num_samples/4)
    rng = np.random.default_rng(seed=seed)
    samples = {i:[] for i in range(4)}
    sample_results = {i:[] for i in range(4)}
    for i,(low,high) in enumerate(zip(quartile[:-1],quartile[1:])):
        print(f"Sampling {_num_samples} from {low=:g} and {high=:g}")
        quantile_mask = (num_reception_edges>=low)&(num_reception_edges<=high)
        mask_size = sum(quantile_mask)
        sample_ids = rng.choice(a=mask_size,size=_num_samples,replace=False)
        _samples = manifestation_ids[quantile_mask][sample_ids]
        _sample_results = num_reception_edges[quantile_mask][sample_ids]
        samples[i].extend(_samples)
        sample_results[i].extend(_sample_results)

    rows =[]
    for quartile in trange(4):
        for doc_id,ground_truth in tqdm(zip(samples[quartile],sample_results[quartile]),total=_num_samples):
            start = time()
            denorm_result = spark.sql(denorm_query.format(doc_id=doc_id)).count()
            denorm_time = time() - start
            
            start = time()
            standard_result = spark.sql(standard_query.format(doc_id=doc_id)).count()
            standard_time = time() - start

            row = {
                "quartile":quartile,
                "trs_id":doc_id,
                "denorm_result":denorm_result,
                "denorm_time":denorm_time,
                "standard_result":standard_result,
                "standard_time":standard_time,
                "ground_truth":ground_truth
            }
            rows.append(row)

    results_df = pd.DataFrame(rows)
    results_df.to_csv("./data/reception_query_results.csv",index=False)
#%%
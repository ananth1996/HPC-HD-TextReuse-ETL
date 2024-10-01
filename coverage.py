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
#%%
textreuses = get_s3("textreuses",processed_bucket)
textreuse_ids = get_s3("textreuse_ids",processed_bucket)
ecco_core = get_s3("ecco_core",raw_bucket)
textreuse_sources = get_s3("textreuse_sources",raw_bucket)
#%%
textreuse_source_lengths = materialise_s3_if_not_exists(
    fname = "textreuse_source_lengths",
    df =spark.sql("""
		SELECT /*+ BROADCAST(ti) */
		trs_id, LENGTH(text) as text_length
		FROM textreuse_sources ts
		INNER JOIN textreuse_ids ti ON ti.text_name = ts.doc_id
		"""),
    bucket=processed_bucket
)
#%%
coverages = materialise_s3_if_not_exists(
    fname="coverages", 
    df= spark.sql("""
WITH groups AS 
(
	SELECT 
		ROW_NUMBER() OVER(PARTITION BY trs1_id,trs2_id ORDER BY trs1_start,trs1_end) AS t1_RN,
		ROW_NUMBER() OVER(PARTITION BY trs1_id,trs2_id ORDER BY trs2_start,trs2_end) AS t2_RN,
		trs1_id,
		trs2_id,
		trs1_start,
		trs1_end,
		trs2_start,
		trs2_end,
		-- LAG(trs1_end,1) OVER (PARTITION BY trs1_id,trs2_id ORDER BY trs1_start,trs1_end) AS t1_previous_end,
		-- LAG(trs2_end,1) OVER (PARTITION BY trs1_id,trs2_id ORDER BY trs2_start,trs2_end) AS t2_previous_end
		MAX(trs1_end) -- largest end date 
			OVER 
			(
				PARTITION BY trs1_id,trs2_id -- group by t1 and t2
				ORDER BY trs1_start,trs1_end  -- order by times
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- FROM all rows before till 1 row before 
			) as t1_previous_end,
		MAX(trs2_end) -- largest end date 
			OVER 
			(
				PARTITION BY trs1_id,trs2_id -- group by t1 and t2
				ORDER BY trs2_start,trs2_end  -- order by times
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- FROM all rows before till 1 row before 
			) as t2_previous_end
		
	FROM textreuses td
), 
islands AS
(
	SELECT 
		*,
		-- debug for checking the t1 islands
		-- CASE WHEN t1_previous_end +1 >= trs1_start THEN 0 ELSE 1 END AS t1_island_start,
		-- previous_end+1 to catch ranges such as (1,6) and (7,12) to become (1,12)
		SUM(CASE WHEN t1_previous_end +1  >= trs1_start THEN 0 ELSE 1 END) OVER (PARTITION BY trs1_id,trs2_id ORDER BY t1_RN) as t1_island_id,
		-- t2 overalps 
		-- debug for checking the islands: 
		-- CASE WHEN t2_previous_end >= trs2_start THEN 0 ELSE 1 END AS t2_island_start,
		SUM(CASE WHEN t2_previous_end+1 >= trs2_start THEN 0 ELSE 1 END) OVER (PARTITION BY trs1_id,trs2_id ORDER BY t2_RN) as t2_island_id
	FROM groups
), 
t1_merged_overlaps as 
(
	SELECT
		trs1_id,
		trs2_id,
		MIN(trs1_start) as trs1_start_pos,
		MAX(trs1_end) as trs1_end_pos,
		MAX(trs1_end) - MIN(trs1_start) as trs1_overlap_length
	FROM islands
	GROUP BY 
		trs1_id,
		trs2_id,
		t1_island_id
),
t1_final as 
(
SELECT 
	trs1_id,
	trs2_id,
	SUM(trs1_overlap_length) as trs1_correct_overlap,
	COUNT(*) as t1_num_merged_hits
FROM t1_merged_overlaps
GROUP BY 
	trs1_id,
	trs2_id
),
t2_merged_overlaps AS 
(
	SELECT
		trs1_id,
		trs2_id,
		MIN(trs2_start) as trs2_start_pos,
		MAX(trs2_end) as trs2_end_pos,
		MAX(trs2_end) - MIN(trs2_start) as trs2_overlap_length
	FROM islands
	GROUP BY 
		trs1_id,
		trs2_id,
		t2_island_id
),
t2_final AS 
(
SELECT 
	trs1_id,
	trs2_id,
	SUM(trs2_overlap_length) as t2_correct_overlap,
	COUNT(*) as t2_num_merged_hits
FROM t2_merged_overlaps
GROUP BY 
	trs1_id,
	trs2_id
),
reuses AS
(
	SELECT 
		t1.trs1_id,
		t1.t1_num_merged_hits as t1_reuses,
		t1.trs1_correct_overlap as reuse_t1_t2,
		t1.trs2_id,
		t2.t2_num_merged_hits as t2_reuses,
		t2.t2_correct_overlap as reuse_t2_t1		
	FROM t1_final t1
	LEFT JOIN t2_final t2 
	ON 
		t1.trs1_id = t2.trs1_id AND
		t1.trs2_id = t2.trs2_id 
)
SELECT 
    /*+ BROADCAST(l1) BROADCAST(l2) */
	r.trs1_id, r.t1_reuses, r.reuse_t1_t2, l1.text_length as t1_length, (r.reuse_t1_t2/l1.text_length)*100 as coverage_t1_t2,
	r.trs2_id, r.t2_reuses, r.reuse_t2_t1, l2.text_length as t2_length, (r.reuse_t2_t1/l2.text_length)*100 as coverage_t2_t1
FROM 
	reuses r
	LEFT JOIN textreuse_source_lengths l1 ON l1.trs_id = r.trs1_id
	LEFT JOIN textreuse_source_lengths l2 ON l2.trs_id = r.trs2_id
"""),
bucket=processed_bucket
)
# %%
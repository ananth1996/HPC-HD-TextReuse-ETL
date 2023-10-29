# %%
from spark_utils import *

# %%

textreuse_source_lengths = get_s3("textreuse_source_lengths",processed_bucket)
reception_edges_denorm = get_s3("reception_edges_denorm",denorm_bucket)
#%%
reception_inception_coverages = materialise_s3(
    fname="reception_inception_coverages", 
    df= spark.sql("""
WITH groups AS 
(
	SELECT 
		ROW_NUMBER() OVER(PARTITION BY src_trs_id,dst_trs_id ORDER BY src_trs_start,src_trs_end) AS t1_RN,
		ROW_NUMBER() OVER(PARTITION BY src_trs_id,dst_trs_id ORDER BY dst_trs_start,dst_trs_end) AS t2_RN,
		src_trs_id AS trs1_id,
		dst_trs_id AS trs2_id,
		src_trs_start AS trs1_start,
		src_trs_end AS trs1_end,
		dst_trs_start AS trs2_start,
		dst_trs_end AS trs2_end,
		MAX(src_trs_end) -- largest end date 
			OVER 
			(
				PARTITION BY src_trs_id,dst_trs_id -- group by t1 and t2
				ORDER BY src_trs_start,src_trs_end  -- order by times
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- FROM all rows before till 1 row before 
			) as t1_previous_end,
		MAX(dst_trs_end) -- largest end date 
			OVER 
			(
				PARTITION BY src_trs_id,dst_trs_id -- group by t1 and t2
				ORDER BY dst_trs_start,dst_trs_end  -- order by times
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- FROM all rows before till 1 row before 
			) as t2_previous_end
		
	FROM reception_edges_denorm red
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
	r.trs1_id AS src_trs_id,
    r.t1_reuses AS num_reuses_src , 
    r.reuse_t1_t2 reuses_src_in_dst,
    l1.text_length as src_length, 
    (r.reuse_t1_t2/l1.text_length)*100 AS coverage_src_in_dst,
	r.trs2_id AS dst_trs_id,
    r.t2_reuses AS num_reuses_dst, 
    r.reuse_t2_t1 AS reuses_dst_in_src, 
    l2.text_length AS dst_length, 
    (r.reuse_t2_t1/l2.text_length)*100 as coverage_dst_in_src
FROM 
	reuses r
	LEFT JOIN textreuse_source_lengths l1 ON l1.trs_id = r.trs1_id
	LEFT JOIN textreuse_source_lengths l2 ON l2.trs_id = r.trs2_id
"""),
bucket=processed_bucket
)
# %%

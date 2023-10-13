INSERT INTO `hpc-hd-newspapers-columnstore`.textreuse_ids
SELECT *
FROM `hpc-hd-newspapers`.textreuse_ids
ORDER BY trs_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.manifestation_ids
SELECT *
FROM `hpc-hd-newspapers`.manifestation_ids
ORDER BY manifestation_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.edition_ids
SELECT *
FROM `hpc-hd-newspapers`.edition_ids
ORDER BY edition_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.work_ids
SELECT *
FROM `hpc-hd-newspapers`.work_ids
ORDER BY work_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.actor_ids
SELECT *
FROM `hpc-hd-newspapers`.actor_ids
ORDER BY actor_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.textreuse_work_mapping
SELECT *
FROM `hpc-hd-newspapers`.textreuse_work_mapping
ORDER BY trs_id,work_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.textreuse_edition_mapping
SELECT *
FROM `hpc-hd-newspapers`.textreuse_edition_mapping
ORDER BY trs_id,edition_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.work_mapping
SELECT *
FROM `hpc-hd-newspapers`.work_mapping
ORDER BY manifestation_id_i,work_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.edition_mapping
SELECT *
FROM `hpc-hd-newspapers`.edition_mapping
ORDER BY manifestation_id_i,edition_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.edition_publication_date
SELECT *
FROM `hpc-hd-newspapers`.edition_publication_date
ORDER BY edition_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.work_earliest_publication_date
SELECT *
FROM `hpc-hd-newspapers`.work_earliest_publication_date
ORDER BY work_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.textreuse_earliest_publication_date
SELECT *
FROM `hpc-hd-newspapers`.textreuse_earliest_publication_date
ORDER BY trs_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.edition_authors
SELECT *
FROM `hpc-hd-newspapers`.edition_authors
ORDER BY edition_id_i;

INSERT INTO `hpc-hd-newspapers-columnstore`.textreuse_source_lengths    
SELECT *
FROM `hpc-hd-newspapers`.textreuse_source_lengths   
ORDER BY trs_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.estc_core    
SELECT *
FROM `hpc-hd-newspapers`.estc_core   
ORDER BY estc_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.ecco_core    
SELECT *
FROM `hpc-hd-newspapers`.ecco_core
ORDER BY ecco_id,estc_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.eebo_core    
SELECT *
FROM `hpc-hd-newspapers`.eebo_core
ORDER BY eebo_id,eebo_tcp_id,estc_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.estc_actors    
SELECT *
FROM `hpc-hd-newspapers`.estc_actors
ORDER BY actor_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.estc_actor_links    
SELECT *
FROM `hpc-hd-newspapers`.estc_actor_links
ORDER BY estc_id,actor_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.newspapers_core    
SELECT *
FROM `hpc-hd-newspapers`.newspapers_core
ORDER BY article_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.defrag_pieces    
SELECT *
FROM `hpc-hd-newspapers`.defrag_pieces
ORDER BY piece_id;
-- FORCE INDEX (PRIMARY)

INSERT INTO `hpc-hd-newspapers-columnstore`.earliest_work_and_pieces_by_cluster    
SELECT *
FROM `hpc-hd-newspapers`.earliest_work_and_pieces_by_cluster
ORDER BY cluster_id,piece_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.earliest_textreuse_by_cluster    
SELECT *
FROM `hpc-hd-newspapers`.earliest_textreuse_by_cluster
ORDER BY cluster_id,trs_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.source_piece_statistics_denorm    
SELECT *
FROM `hpc-hd-newspapers`.source_piece_statistics_denorm
ORDER BY piece_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.non_source_pieces    
SELECT *
FROM `hpc-hd-newspapers`.non_source_pieces
ORDER BY cluster_id,piece_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.clustered_defrag_pieces    
SELECT *
FROM `hpc-hd-newspapers`.clustered_defrag_pieces
ORDER BY piece_id,cluster_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.coverages    
SELECT *
FROM `hpc-hd-newspapers`.coverages
ORDER BY trs1_id,trs2_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.reception_edges_denorm    
SELECT *
FROM `hpc-hd-newspapers`.reception_edges_denorm
ORDER BY src_trs_id,dst_trs_end;

INSERT INTO `hpc-hd-newspapers-columnstore`.defrag_textreuses
SELECT *
FROM `hpc-hd-newspapers`.defrag_textreuses
ORDER BY textreuse_id;

INSERT INTO `hpc-hd-newspapers-columnstore`.textreuse_sources
SELECT *
FROM `hpc-hd-newspapers`.textreuse_sources
ORDER BY trs_id;



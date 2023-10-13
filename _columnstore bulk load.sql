INSERT INTO `hpc-hd-columnstore`.textreuse_ids
SELECT *
FROM `hpc-hd`.textreuse_ids
ORDER BY trs_id;

INSERT INTO `hpc-hd-columnstore`.manifestation_ids
SELECT *
FROM `hpc-hd`.manifestation_ids
ORDER BY manifestation_id_i;

INSERT INTO `hpc-hd-columnstore`.edition_ids
SELECT *
FROM `hpc-hd`.edition_ids
ORDER BY edition_id_i;

INSERT INTO `hpc-hd-columnstore`.work_ids
SELECT *
FROM `hpc-hd`.work_ids
ORDER BY work_id_i;

INSERT INTO `hpc-hd-columnstore`.actor_ids
SELECT *
FROM `hpc-hd`.actor_ids
ORDER BY actor_id_i;

INSERT INTO `hpc-hd-columnstore`.textreuse_work_mapping
SELECT *
FROM `hpc-hd`.textreuse_work_mapping
ORDER BY trs_id,work_id_i;

INSERT INTO `hpc-hd-columnstore`.textreuse_edition_mapping
SELECT *
FROM `hpc-hd`.textreuse_edition_mapping
ORDER BY trs_id,edition_id_i;

INSERT INTO `hpc-hd-columnstore`.work_mapping
SELECT *
FROM `hpc-hd`.work_mapping
ORDER BY manifestation_id_i,work_id_i;

INSERT INTO `hpc-hd-columnstore`.edition_mapping
SELECT *
FROM `hpc-hd`.edition_mapping
ORDER BY manifestation_id_i,edition_id_i;

INSERT INTO `hpc-hd-columnstore`.edition_publication_year
SELECT *
FROM `hpc-hd`.edition_publication_year
ORDER BY edition_id_i;

INSERT INTO `hpc-hd-columnstore`.work_earliest_publication_year
SELECT *
FROM `hpc-hd`.work_earliest_publication_year
ORDER BY work_id_i;

INSERT INTO `hpc-hd-columnstore`.textreuse_earliest_publication_year
SELECT *
FROM `hpc-hd`.textreuse_earliest_publication_year
ORDER BY trs_id;

INSERT INTO `hpc-hd-columnstore`.edition_authors
SELECT *
FROM `hpc-hd`.edition_authors
ORDER BY edition_id_i;

-- INSERT INTO `hpc-hd-columnstore`.textreuse_source_lengths    
-- SELECT *
-- FROM `hpc-hd`.textreuse_source_lengths   
-- ORDER BY trs_id;

INSERT INTO `hpc-hd-columnstore`.estc_core    
SELECT *
FROM `hpc-hd`.estc_core   
ORDER BY estc_id;

INSERT INTO `hpc-hd-columnstore`.ecco_core    
SELECT *
FROM `hpc-hd`.ecco_core
ORDER BY ecco_id,estc_id;

INSERT INTO `hpc-hd-columnstore`.eebo_core    
SELECT *
FROM `hpc-hd`.eebo_core
ORDER BY eebo_id,eebo_tcp_id,estc_id;

INSERT INTO `hpc-hd-columnstore`.estc_actors    
SELECT *
FROM `hpc-hd`.estc_actors
ORDER BY actor_id;

INSERT INTO `hpc-hd-columnstore`.estc_actor_links    
SELECT *
FROM `hpc-hd`.estc_actor_links
ORDER BY estc_id,actor_id;

-- INSERT INTO `hpc-hd-columnstore`.newspapers_core    
-- SELECT *
-- FROM `hpc-hd`.newspapers_core
-- ORDER BY article_id;

INSERT INTO `hpc-hd-columnstore`.defrag_pieces    
SELECT *
FROM `hpc-hd`.defrag_pieces
ORDER BY piece_id;
-- FORCE INDEX (PRIMARY)

INSERT INTO `hpc-hd-columnstore`.earliest_work_and_pieces_by_cluster    
SELECT *
FROM `hpc-hd`.earliest_work_and_pieces_by_cluster
ORDER BY cluster_id,piece_id;

INSERT INTO `hpc-hd-columnstore`.earliest_textreuse_by_cluster    
SELECT *
FROM `hpc-hd`.earliest_textreuse_by_cluster
ORDER BY cluster_id,trs_id;

INSERT INTO `hpc-hd-columnstore`.source_piece_statistics_denorm    
SELECT *
FROM `hpc-hd`.source_piece_statistics_denorm
ORDER BY piece_id;

INSERT INTO `hpc-hd-columnstore`.non_source_pieces    
SELECT *
FROM `hpc-hd`.non_source_pieces
ORDER BY cluster_id,piece_id;

INSERT INTO `hpc-hd-columnstore`.clustered_defrag_pieces    
SELECT *
FROM `hpc-hd`.clustered_defrag_pieces
ORDER BY piece_id,cluster_id;

-- INSERT INTO `hpc-hd-columnstore`.coverages    
-- SELECT *
-- FROM `hpc-hd`.coverages
-- ORDER BY trs1_id,trs2_id;

INSERT INTO `hpc-hd-columnstore`.reception_edges_denorm    
SELECT *
FROM `hpc-hd`.reception_edges_denorm
ORDER BY src_trs_id,dst_trs_end;

INSERT INTO `hpc-hd-columnstore`.defrag_textreuses
SELECT *
FROM `hpc-hd`.defrag_textreuses
ORDER BY textreuse_id;



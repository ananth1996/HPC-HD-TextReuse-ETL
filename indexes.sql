-- metadata Indices 

ALTER TABLE `textreuse_ids`
ADD PRIMARY KEY (`trs_id`),
ADD INDEX IF NOT EXISTS manifestation_trs_composite (`manifestation_id`,`trs_id`);

ALTER TABLE manifestation_ids
ADD PRIMARY KEY (`manifestation_id_i`),
ADD INDEX IF NOT EXISTS `manifestation_covering` (`manifestation_id`,`manifestation_id_i`);

ALTER TABLE `edition_ids`
ADD PRIMARY KEY (`edition_id_i`),
ADD INDEX IF NOT EXISTS `edition_covering` (`edition_id`,`edition_id_i`);

ALTER TABLE `work_ids`
ADD PRIMARY KEY (`work_id_i`),
ADD INDEX IF NOT EXISTS `work_covering` (`work_id`,`work_id_i`);

ALTER TABLE `actor_ids`
ADD PRIMARY KEY (`actor_id_i`),
ADD INDEX IF NOT EXISTS `actor_composite` (`actor_id`,`actor_id_i`);

ALTER TABLE `edition_authors`
ADD INDEX IF NOT EXISTS `edition_id_i` (`edition_id_i`),
ADD INDEX IF NOT EXISTS `actor_id_i` (`actor_id_i`);

ALTER TABLE `textreuse_work_mapping`
ADD INDEX IF NOT EXISTS `trs_id` (`trs_id`),
ADD INDEX IF NOT EXISTS `work_id_i` (`work_id_i`);

ALTER TABLE `textreuse_edition_mapping`
ADD INDEX IF NOT EXISTS `trs_id` (`trs_id`),
ADD INDEX IF NOT EXISTS `edition_id_i` (`edition_id_i`);

ALTER TABLE `work_mapping`
ADD INDEX IF NOT EXISTS `manifestation_id_i` (`manifestation_id_i`),
ADD INDEX IF NOT EXISTS `work_id_i` (`work_id_i`);

ALTER TABLE `edition_mapping`
ADD INDEX IF NOT EXISTS `manifestation_id_i` (`manifestation_id_i`),
ADD INDEX IF NOT EXISTS `edition_id_i` (`edition_id_i`);

ALTER TABLE `edition_publication_date`
-- There might be editions with several possible publication dates
ADD INDEX IF NOT EXISTS `edition_covering` (`edition_id_i`,`publication_date`);

ALTER TABLE `work_earliest_publication_date`
ADD PRIMARY KEY (`work_id_i`);

ALTER TABLE `textreuse_earliest_publication_date`
ADD PRIMARY KEY (`trs_id`);

ALTER TABLE `textreuse_source_lengths`
ADD PRIMARY KEY (`trs_id`);

-- raw tables 

ALTER TABLE `estc_core`
ADD PRIMARY KEY (`estc_id`),
ADD INDEX IF NOT EXISTS `work_id` (`work_id`(575));

ALTER TABLE `ecco_core`
ADD PRIMARY KEY (`ecco_id`),
ADD INDEX IF NOT EXISTS `estc_id` (`estc_id`);

ALTER TABLE `eebo_core`
ADD INDEX IF NOT EXISTS `eebo_id` (`eebo_id`),
ADD INDEX IF NOT EXISTS `eebo_tcp_id` (`eebo_tcp_id`),
ADD INDEX IF NOT EXISTS `estc_id` (`estc_id`);

ALTER TABLE `estc_actors`
ADD PRIMARY KEY (`actor_id`);

ALTER TABLE `estc_actor_links`
ADD INDEX IF NOT EXISTS `estc_id` (`estc_id`),
ADD INDEX IF NOT EXISTS `actor_id` (`actor_id`),
ADD INDEX IF NOT EXISTS `actor_name_primary` (`actor_name_primary`(575));

ALTER TABLE `newspapers_core`
ADD PRIMARY KEY (`article_id`);

-- Data Indices 
ALTER TABLE `defrag_pieces`
ADD PRIMARY KEY (`piece_id`),
ADD INDEX IF NOT EXISTS `trs_composite` (`trs_id`,`piece_id`);

ALTER TABLE `defrag_textreuses`
ADD PRIMARY KEY (`textreuse_id`),
ADD INDEX IF NOT EXISTS `composite1` (`piece1_id`,`piece2_id`),
ADD INDEX IF NOT EXISTS `composite2` (`piece2_id`,`piece1_id`);

ALTER TABLE `clustered_defrag_pieces` 
ADD PRIMARY KEY (`piece_id`),
ADD INDEX IF NOT EXISTS `cluster_covering` (`cluster_id`,`piece_id`);

ALTER TABLE `earliest_textreuse_by_cluster`
ADD INDEX IF NOT EXISTS `cluster_covering` (`cluster_id`,`trs_id`),
ADD INDEX IF NOT EXISTS `trs_covering` (`trs_id`,`cluster_id`);

ALTER TABLE `earliest_work_and_pieces_by_cluster`
ADD INDEX IF NOT EXISTS `cluster_composite1` (`cluster_id`,`piece_id`),
ADD INDEX IF NOT EXISTS `cluster_composite2` (`cluster_id`,`work_id_i`),
ADD INDEX IF NOT EXISTS `cluster_covering` (`piece_id`,`cluster_id`,`work_id_i`);

ALTER TABLE `reception_edges_denorm`
ADD INDEX IF NOT EXISTS `src_trs_id` (`src_trs_id`),
ADD INDEX IF NOT EXISTS `dst_trs_id` (`dst_trs_id`);

ALTER TABLE `reception_edges`
ADD INDEX IF NOT EXISTS `src` (`src_piece_id`),
ADD INDEX IF NOT EXISTS `dst` (`dst_piece_id`);

ALTER TABLE `coverages`
ADD INDEX IF NOT EXISTS `trs1_composite` (`trs1_id`,`trs2_id`),
ADD INDEX IF NOT EXISTS `trs2_composite` (`trs2_id`,`trs1_id`);

ALTER TABLE `source_piece_statistics_denorm`
ADD INDEX IF NOT EXISTS `piece_id` (`piece_id`),
ADD INDEX IF NOT EXISTS `edition_id_i` (`edition_id_i`),
ADD INDEX IF NOT EXISTS `trs_id` (`trs_id`),
ADD INDEX IF NOT EXISTS `piece_length` (`piece_length`);

ALTER TABLE `source_piece_statistics`
ADD PRIMARY KEY (`piece_id`),
ADD INDEX IF NOT EXISTS `piece_length` (`piece_length`);

ALTER TABLE `textreuse_sources`
ADD PRIMARY KEY (`trs_id`);

ALTER TABLE `reception_inception_coverages`
ADD INDEX IF NOT EXISTS `src_trs_id` (`src_trs_id`),
ADD INDEX IF NOT EXISTS `reception` (`coverage_src_in_dst`),
ADD INDEX IF NOT EXISTS `dst_trs_id` (`dst_trs_id`);
ADD INDEX IF NOT EXISTS `inception` (`coverage_dst_in_src`),




CREATE TABLE IF NOT EXISTS `textreuse_manifestation_mapping`(
	`trs_id` int(11) unsigned NOT NULL,
    `manifestation_id_i` int(11) unsigned NOT NULL
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

INSERT INTO textreuse_manifestation_mapping
SELECT trs_id,manifestation_id_i FROM textreuse_ids ti 
INNER JOIN manifestation_ids mi USING(manifestation_id)

ALTER TABLE `textreuse_manifestation_mapping`
ADD INDEX IF NOT EXISTS `trs_id` (`trs_id`),
ADD INDEX IF NOT EXISTS `manifestation_id_i` (`manifestation_id_i`);
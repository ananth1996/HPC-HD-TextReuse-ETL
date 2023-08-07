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

ALTER TABLE `edition_publication_year`
-- There might be editions with several possible publication dates
ADD INDEX IF NOT EXISTS `edition_covering` (`edition_id_i`,`publication_year`);

ALTER TABLE `work_earliest_publication_year`
ADD PRIMARY KEY (`work_id_i`);

ALTER TABLE `textreuse_earliest_publication_year`
ADD PRIMARY KEY (`trs_id`);


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

ALTER TABLE reception_edges_denorm
ADD INDEX IF NOT EXISTS `src_trs_id` (`src_trs_id`),
ADD INDEX IF NOT EXISTS `dst_trs_id` (`dst_trs_id`);
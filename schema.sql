-- metadata tables

CREATE TABLE IF NOT EXISTS `textreuse_ids` (
    `trs_id` int(11) unsigned NOT NULL,
    `text_name` varchar(100),
    `manifestation_id` varchar(100),
    `structure_name` varchar(100)
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `manifestation_ids`(
    `manifestation_id_i` int(11) unsigned NOT NULL,
    `manifestation_id` varchar(100)
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `edition_ids`(
    `edition_id_i` int(11) unsigned NOT NULL,
    `edition_id` varchar(100)
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `work_ids`(
    `work_id_i` int(11) unsigned NOT NULL,
    `work_id` varchar(100)
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `actor_ids`(
    `actor_id_i` int(11) unsigned NOT NULL,
    `actor_id` varchar(100),
    `name_unified` text
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `textreuse_work_mapping`(
    `trs_id` int(11) unsigned NOT NULL,
    `work_id_i` int(11) unsigned NOT NULL
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `textreuse_edition_mapping`(
    `trs_id` int(11) unsigned NOT NULL,
    `edition_id_i` int(11) unsigned NOT NULL
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `work_mapping`(
    `manifestation_id_i` int(11) unsigned NOT NULL,
    `work_id_i` int(11) unsigned NOT NULL
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `edition_mapping`(
    `manifestation_id_i` int(11) unsigned NOT NULL,
    `edition_id_i` int(11) unsigned NOT NULL
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `edition_publication_year` (
    `edition_id_i` int(11) unsigned NOT NULL,
    `publication_year` int(4) unsigned DEFAULT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `work_earliest_publication_year` (
    `work_id_i` int(11) unsigned NOT NULL,
    `publication_year` int(4) DEFAULT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `textreuse_earliest_publication_year` (
    `trs_id` int(11) unsigned NOT NULL,
    `publication_year` int(4) DEFAULT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `edition_authors` (
    `edition_id_i` int(11) unsigned NOT NULL,
    `actor_id_i` int(11) unsigned DEFAULT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

-- Data Tables 

CREATE TABLE IF NOT EXISTS `defrag_pieces` (
    `piece_id` bigint(20) unsigned NOT NULL,
    `trs_id` int(11) unsigned NOT NULL,
    `trs_start` int(11) unsigned NOT NULL,
    `trs_end` int(11) unsigned NOT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `defrag_textreuses` (
    `textreuse_id` bigint(20) unsigned NOT NULL,
    `piece1_id` bigint(20) unsigned NOT NULL,
    `piece2_id` bigint(20) unsigned NOT NULL,
    `num_orig_links` int(11) unsigned NOT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `clustered_defrag_pieces` (
    `piece_id` bigint(20) unsigned NOT NULL,
    `cluster_id` int(11) unsigned NOT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `earliest_textreuse_by_cluster` (
    `cluster_id` int(11) unsigned NOT NULL,
    `trs_id` int(11) unsigned NOT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `earliest_work_and_pieces_by_cluster` (
    `cluster_id` int(11) unsigned NOT NULL,
    `work_id_i` int(11) unsigned NOT NULL,
    `piece_id` bigint(20) unsigned NOT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `reception_edges_denorm` (
  `src_trs_id` int(11) unsigned NOT NULL,
  `src_trs_start` int(11) unsigned NOT NULL,
  `src_trs_end` int(11) unsigned NOT NULL,
  `dst_trs_id` int(11) unsigned NOT NULL,
  `dst_trs_start` int(11) unsigned NOT NULL,
  `dst_trs_end` int(11) unsigned NOT NULL
) ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `source_piece_statistics_denorm` (
    `piece_id` bigint(20) unsigned NOT NULL,
    `cluster_id` int(11) unsigned NOT NULL,
    `trs_id` int(11) unsigned NOT NULL,
    `piece_length` int(11) unsigned NOT NULL,
    `num_reception_edges` bigint(20) unsigned NOT NULL,
    `num_different_work_ids` int(11) unsigned NOT NULL,
    `num_work_ids_different_authors` int(11) unsigned NOT NULL,
    `trs_start` int(11) unsigned NOT NULL,
    `trs_end` int(11) unsigned NOT NULL,
    `edition_id_i` int(11) unsigned NOT NULL
)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0;
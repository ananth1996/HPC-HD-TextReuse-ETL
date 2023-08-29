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

-- raw metadata tables 

CREATE TABLE IF NOT EXISTS `estc_core` (
  `language_primary` varchar(30) DEFAULT NULL,
  `estc_id` varchar(7) DEFAULT NULL,
  `gatherings.original` varchar(6) DEFAULT NULL,
  `width.original` tinyint(4) DEFAULT NULL,
  `height.original` double DEFAULT NULL,
  `obl.original` tinyint(4) DEFAULT NULL,
  `gatherings` varchar(6) DEFAULT NULL,
  `width` double DEFAULT NULL,
  `height` double DEFAULT NULL,
  `obl` double DEFAULT NULL,
  `area` double DEFAULT NULL,
  `original` text DEFAULT NULL,
  `publication_year_from` double DEFAULT NULL,
  `publication_year_till` double DEFAULT NULL,
  `publication_year` double DEFAULT NULL,
  `publication_decade` double DEFAULT NULL,
  `publication_century` double DEFAULT NULL,
  `uncertain` tinyint(4) DEFAULT NULL,
  `circa` tinyint(4) DEFAULT NULL,
  `range` tinyint(4) DEFAULT NULL,
  `price` text DEFAULT NULL,
  `prices_found` varchar(38) DEFAULT NULL,
  `Pound` double DEFAULT NULL,
  `Shilling` double DEFAULT NULL,
  `Pence` double DEFAULT NULL,
  `total_price` double DEFAULT NULL,
  `year` double DEFAULT NULL,
  `Commodity` varchar(5) DEFAULT NULL,
  `Market` varchar(6) DEFAULT NULL,
  `variety` varchar(7) DEFAULT NULL,
  `currancy` varchar(13) DEFAULT NULL,
  `volume` varchar(4) DEFAULT NULL,
  `C.currancy` varchar(5) DEFAULT NULL,
  `C.volume` varchar(4) DEFAULT NULL,
  `value` double DEFAULT NULL,
  `notes` varchar(5) DEFAULT NULL,
  `source` varchar(9) DEFAULT NULL,
  `total_price_in_silver` double DEFAULT NULL,
  `freq` varchar(18) DEFAULT NULL,
  `annual` double DEFAULT NULL,
  `is_periodical` tinyint(4) DEFAULT NULL,
  `short_title` text DEFAULT NULL,
  `work_id` text DEFAULT NULL,
  `publication_place` varchar(24) DEFAULT NULL,
  `publication_country` varchar(21) DEFAULT NULL,
  `false_imprint` tinyint(4) DEFAULT NULL,
  `org_260_a` varchar(246) DEFAULT NULL,
  `org_260_a_square_brackets` tinyint(4) DEFAULT NULL,
  `org_752_a` varchar(209) DEFAULT NULL,
  `org_752_b` varchar(185) DEFAULT NULL,
  `org_752_d` varchar(166) DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `latitude` double DEFAULT NULL,
  `geo_id` varchar(72) DEFAULT NULL,
  `pagecount.multiplier` double DEFAULT NULL,
  `pagecount.squarebracket` double DEFAULT NULL,
  `pagecount.plate` double DEFAULT NULL,
  `pagecount.arabic` double DEFAULT NULL,
  `pagecount.roman` double DEFAULT NULL,
  `pagecount.sheet` double DEFAULT NULL,
  `pagecount` double DEFAULT NULL,
  `volnumber` double DEFAULT NULL,
  `volcount` double DEFAULT NULL,
  `parts` double DEFAULT NULL,
  `pagecount_from` varchar(34) DEFAULT NULL,
  `pagecount.orig` double DEFAULT NULL,
  `singlevol` tinyint(4) DEFAULT NULL,
  `multivol` tinyint(4) DEFAULT NULL,
  `issue` tinyint(4) DEFAULT NULL,
  `document.items` double DEFAULT NULL,
  `paper` double DEFAULT NULL,
  `document_type` varchar(10) DEFAULT NULL
) ENGINE=Aria DEFAULT CHARSET=utf8mb4 PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `ecco_core` (
  `ecco_id` varchar(10) DEFAULT NULL,
  `estc_id` varchar(7) DEFAULT NULL,
  `ecco_part` varchar(5) DEFAULT NULL,
  `ecco_module` varchar(32) DEFAULT NULL,
  `ecco_full_title` text DEFAULT NULL,
  `estc_id_octavo` varchar(7) DEFAULT NULL,
  `ecco_nr_characters` double DEFAULT NULL,
  `ecco_nr_tokens` double DEFAULT NULL,
  `ecco_date_start` double DEFAULT NULL,
  `ecco_date_end` double DEFAULT NULL,
  `ecco_pages` double DEFAULT NULL,
  `ecco_languge` varchar(29) DEFAULT NULL,
  `ecco_nr_paragraphs` double DEFAULT NULL
) ENGINE=Aria DEFAULT CHARSET=utf8mb4 PAGE_CHECKSUM=0 TRANSACTIONAL=0;

CREATE TABLE IF NOT EXISTS `eebo_core` (
  `eebo_id` varchar(9) DEFAULT NULL,
  `eebo_tcp_id` varchar(6) DEFAULT NULL,
  `estc_id` varchar(23) DEFAULT NULL,
  `marc_full_title` text DEFAULT NULL,
  `eebo_tls_publication_type` varchar(5) DEFAULT NULL,
  `eebo_tls_collection` varchar(37) DEFAULT NULL,
  `eebo_tls_title` text DEFAULT NULL,
  `eebo_tls_author` text DEFAULT NULL,
  `eebo_tls_publisher` text DEFAULT NULL,
  `eebo_tls_publication_date` varchar(18) DEFAULT NULL,
  `eebo_tls_language` varchar(30) DEFAULT NULL,
  `eebo_tls_source_library` varchar(67) DEFAULT NULL,
  `eebo_tls_publication_country` varchar(16) DEFAULT NULL,
  `proquest_url` varchar(51) DEFAULT NULL
) ENGINE=Aria DEFAULT CHARSET=utf8mb4 PAGE_CHECKSUM=0 TRANSACTIONAL=0;


CREATE TABLE IF NOT EXISTS `estc_actors` (
  `actor_id` varchar(95) DEFAULT NULL,
  `actor_id_type` varchar(8) DEFAULT NULL,
  `old_actor_ids` text DEFAULT NULL,
  `bbti_link` varchar(53) DEFAULT NULL,
  `viaf_link` varchar(44) DEFAULT NULL,
  `is_organization` tinyint(4) DEFAULT NULL,
  `name_unified` text DEFAULT NULL,
  `name_unified_source` varchar(20) DEFAULT NULL,
  `nametype` varchar(7) DEFAULT NULL,
  `name_first` varchar(21) DEFAULT NULL,
  `name_last` varchar(24) DEFAULT NULL,
  `name_remainder` varchar(17) DEFAULT NULL,
  `name_variants` text DEFAULT NULL,
  `names_for_gender` varchar(75) DEFAULT NULL,
  `actor_titles` tinyint(4) DEFAULT NULL,
  `actor_gender` varchar(14) DEFAULT NULL,
  `actor_gender_source` varchar(19) DEFAULT NULL,
  `year_birth_viaf` double DEFAULT NULL,
  `year_death_viaf` double DEFAULT NULL,
  `year_bio_start_bbti` double DEFAULT NULL,
  `year_bio_end_bbti` double DEFAULT NULL,
  `year_birth_estc` double DEFAULT NULL,
  `year_death_estc` double DEFAULT NULL,
  `year_birth` double DEFAULT NULL,
  `year_death` double DEFAULT NULL,
  `year_birth_source` varchar(4) DEFAULT NULL,
  `year_death_source` varchar(4) DEFAULT NULL,
  `year_active_first_estc` double DEFAULT NULL,
  `year_active_last_estc` double DEFAULT NULL,
  `year_active_first_bbti` double DEFAULT NULL,
  `year_active_last_bbti` tinyint(4) DEFAULT NULL,
  `year_pub_first_estc` double DEFAULT NULL,
  `year_pub_last_estc` double DEFAULT NULL,
  `actor_group` varchar(9) DEFAULT NULL
) ENGINE=Aria DEFAULT CHARSET=utf8mb4 PAGE_CHECKSUM=0 TRANSACTIONAL=0;


CREATE TABLE `estc_actor_links` (
  `estc_id` varchar(7) DEFAULT NULL,
  `actor_id` varchar(95) DEFAULT NULL,
  `actor_id_methods` varchar(158) DEFAULT NULL,
  `source_tags` varchar(13) DEFAULT NULL,
  `actor_name_primary` text DEFAULT NULL,
  `actor_names_other` text DEFAULT NULL,
  `actor_is_anonymous` tinyint(4) DEFAULT NULL,
  `actor_roles_all` varchar(52) DEFAULT NULL,
  `actor_addresses` text DEFAULT NULL,
  `actor_ids_old` varchar(63) DEFAULT NULL,
  `actor_role_author` tinyint(4) DEFAULT NULL,
  `actor_role_printer` tinyint(4) DEFAULT NULL,
  `actor_role_publisher` tinyint(4) DEFAULT NULL,
  `actor_role_bookseller` tinyint(4) DEFAULT NULL,
  `actor_role_unknown` tinyint(4) DEFAULT NULL,
  `actor_role_corporate_author` tinyint(4) DEFAULT NULL,
  `actor_role_geographic_record` tinyint(4) DEFAULT NULL,
  `actor_role_corporate_unknown` tinyint(4) DEFAULT NULL,
  `actor_role_translator` tinyint(4) DEFAULT NULL,
  `actor_role_attributed_name` tinyint(4) DEFAULT NULL,
  `actor_role_editor` tinyint(4) DEFAULT NULL,
  `actor_role_engraver` tinyint(4) DEFAULT NULL,
  `actor_role_other` tinyint(4) DEFAULT NULL,
  `brackets_in_booktrade_name` tinyint(4) DEFAULT NULL,
  `manually_unified` tinyint(4) DEFAULT NULL,
  `primary_publisher` tinyint(4) DEFAULT NULL
) ENGINE=Aria DEFAULT CHARSET=utf8mb4 PAGE_CHECKSUM=0 TRANSACTIONAL=0;

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
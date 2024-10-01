# For non_source_pieces

## RowStore

### Creating 

```sql
MariaDB [hpc-hd-newspapers]> CREATE TABLE `temp` (`cluster_id` int(11) unsigned NOT NULL,`piece_id` bigint(20) unsigned NOT NULL)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0
    -> SELECT cluster_id,piece_id FROM
    -> earliest_work_and_pieces_by_cluster
    -> RIGHT JOIN clustered_defrag_pieces cdp
    -> USING(cluster_id,piece_id)
    -> -- where it is not the earliest piece
    -> WHERE work_id_i IS NULL;
Query OK, 988122521 rows affected (1 hour 34 min 13.832 sec)
Records: 988122521  Duplicates: 0  Warnings: 0
```

Indexing 

```sql
MariaDB [hpc-hd-newspapers] > ALTER TABLE `non_source_pieces` 
    ADD UNIQUE KEY `cluster_covering` (`cluster_id`,`piece_id`),
    ADD UNIQUE KEY `piece_covering` (`piece_id`,`cluster_id`);
Query OK, 988122521 rows affected (46 min 46.949 sec)
Records: 988122521  Duplicates: 0  Warnings: 0
```

## Columnstore
MariaDB [hpc-hd-newspapers-columnstore]>     INSERT INTO `temp`
    ->     SELECT cluster_id,piece_id FROM
    ->     earliest_work_and_pieces_by_cluster
    ->     RIGHT JOIN clustered_defrag_pieces cdp
    ->     USING(cluster_id,piece_id)
    ->     -- where it is not the earliest piece
    ->     WHERE work_id_i IS NULL
    ->     ORDER BY cluster_id,piece_id;
Internal error: TupleAnnexStep::executeParallelOrderBy() MCS-2018: Not enough memory to process the LIMIT.  Consider raising TotalUmMemory or reducing memory usage.

## Spark 

### Creating asset
2 min 2 sec 


### Loading to Aria Tables 

1455.925269602798 seconds

### Indexing in Aria 

3096.556833673734 seconds


### Columnstore Bulk Loading 
16 min 6.418 sec



# Source piece source_piece_statistics_denorm

## Aria 

```sql
MariaDB [hpc-hd-newspapers]> INSERT INTO temp (piece_id,cluster_id,trs_id,piece_length,num_reception_edges,num_different_work_ids,num_work_ids_different_authors,trs_start,trs_end,edition_id_i)
    -> WITH source_piece_statistics AS (
    -> SELECT
    ->     src_piece_id AS piece_id,
    ->     MIN(cdp.cluster_id) AS cluster_id,
    ->     MIN(dp_src.trs_end)-MIN(dp_src.trs_start) as piece_length,
    ->     COUNT(*) AS num_reception_edges,
    ->     COUNT(
    ->         DISTINCT
    ->         CASE
    ->             WHEN twm_src.work_id_i <> twm_dst.work_id_i THEN twm_dst.work_id_i
    ->             ELSE NULL
    ->         END ) AS num_different_work_ids,
    ->     COUNT(
    ->         DISTINCT
    ->         (CASE
    ->             -- if source has an author
    ->             WHEN ea_src.actor_id_i IS NOT NULL AND
    ->             -- and the destination author is different or NULL
    ->             (ea_src.actor_id_i <> ea_dst.actor_id_i OR ea_dst.actor_id_i IS NULL)
    ->             -- count the work_id of the destination
    ->             THEN twm_dst.work_id_i
    ->             -- if source author is not there then count destination works
    ->             WHEN ea_src.actor_id_i IS NULL THEN twm_dst.work_id_i
    ->             ELSE NULL
    ->         END)) AS num_work_ids_different_authors
    -> FROM reception_edges re
    -> INNER JOIN defrag_pieces dp_src ON re.src_piece_id = dp_src.piece_id
    -> INNER JOIN textreuse_edition_mapping tem_src ON tem_src.trs_id = dp_src.trs_id
    -> INNER JOIN edition_authors ea_src ON ea_src.edition_id_i = tem_src.edition_id_i
    -> INNER JOIN textreuse_work_mapping twm_src ON twm_src.trs_id = dp_src.trs_id
    -> INNER JOIN clustered_defrag_pieces cdp ON re.src_piece_id = cdp.piece_id
    -> -- destination pieces
    -> INNER JOIN defrag_pieces dp_dst ON re.dst_piece_id = dp_dst.piece_id
    -> INNER JOIN textreuse_edition_mapping tem_dst ON tem_dst.trs_id = dp_dst.trs_id
    -> INNER JOIN edition_authors ea_dst ON ea_dst.edition_id_i = tem_dst.edition_id_i
    -> INNER JOIN textreuse_work_mapping twm_dst ON twm_dst.trs_id = dp_dst.trs_id
    -> GROUP BY src_piece_id
    -> )
    -> SELECT piece_id,cluster_id,trs_id,piece_length,num_reception_edges,num_different_work_ids,num_work_ids_different_authors,trs_start,trs_end,edition_id_i
    -> FROM
    ->     source_piece_statistics
    ->     INNER JOIN defrag_pieces USING(piece_id)
    ->     INNER JOIN textreuse_edition_mapping USING(trs_id);

^CCtrl-C -- query killed. Continuing normally.
ERROR 1317 (70100): Query execution was interrupted
```

Took > 135626 seconds
## Spark 

### Asset creation 

source_piece_statistics: 22 min 18 sec
source_piece_statistics_denorm: 2 min 14 sec

### Bulk loading 
3.5 min

### Indexing in Aria 

7 min 9.914 sec

### Columnstore Bulk Loading 

5 min 13.100 sec

# For reception_edges_denorm 


## Columnstore 

MariaDB [hpc-hd-newspapers-columnstore]> INSERT INTO temp (src_trs_id,src_trs_start,src_trs_end,dst_trs_id,dst_trs_start,dst_trs_end)
    -> WITH reception_edges AS (
    ->     SELECT ewapbca.piece_id as src_piece_id, nsp.piece_id as dst_piece_id
    ->     FROM earliest_work_and_pieces_by_cluster ewapbca
    ->     INNER JOIN non_source_pieces nsp USING(cluster_id)
    -> )
    -> SELECT
    ->     dp1.trs_id AS src_trs_id,
    ->     dp1.trs_start AS src_trs_start,
    ->     dp1.trs_end AS src_trs_end,
    ->     dp2.trs_id AS dst_trs_id,
    ->     dp2.trs_start AS dst_trs_start,
    ->     dp2.trs_end AS dst_trs_end
    -> FROM
    -> reception_edges re
    -> INNER JOIN defrag_pieces dp1 ON re.src_piece_id = dp1.piece_id
    -> INNER JOIN defrag_pieces dp2 ON re.dst_piece_id = dp2.piece_id;
ERROR 1815 (HY000): Internal error: (437) MCS-2001: Join or subselect exceeds memory limit.

## Rowstore 


## Spark 


### Asset creation 

- reception_edges : 4 min 53 sec
- reception_edges_denorm: 9 min 19 sec

### Bulk Loading 

2.5 hours

### Aria Index Creation 

4 hours 19 min 10.855 sec

### Columnstore Bulk Loading 

7 hours 41 min 59.124 sec




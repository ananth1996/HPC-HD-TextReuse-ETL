# For non_source_pieces

## RowStore

### Creating 
MariaDB [hpc-hd-newspapers]> CREATE TABLE `temp` (`cluster_id` int(11) unsigned NOT NULL,`piece_id` bigint(20) unsigned NOT NULL)ENGINE=Aria PAGE_CHECKSUM=0 TRANSACTIONAL=0
    -> SELECT cluster_id,piece_id FROM
    -> earliest_work_and_pieces_by_cluster
    -> RIGHT JOIN clustered_defrag_pieces cdp
    -> USING(cluster_id,piece_id)
    -> -- where it is not the earliest piece
    -> WHERE work_id_i IS NULL;
Query OK, 988122521 rows affected (1 hour 34 min 13.832 sec)
Records: 988122521  Duplicates: 0  Warnings: 0


- 

MariaDB [hpc-hd-newspapers] > ALTER TABLE `non_source_pieces` 
    ADD UNIQUE KEY `cluster_covering` (`cluster_id`,`piece_id`),
    ADD UNIQUE KEY `piece_covering` (`piece_id`,`cluster_id`);
Query OK, 988122521 rows affected (46 min 46.949 sec)
Records: 988122521  Duplicates: 0  Warnings: 0


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

3096.556833673734	seconds


### Columnstore Bulk Loading 
16 min 6.418 sec



# Source piece source_piece_statistics_denorm


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
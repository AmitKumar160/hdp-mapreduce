# hdp-mapreduce
Mapreduce jobs

Note: Please create schema and table according to feilds selected in Mapper class
1. Get property xml files from Hadoop cluster
2.package jar using below command
jar uf hdp-mapreduce-0.0.1-SNAPSHOT.jar hive-site.xml hdfs-site.xml hbase-site.xml core-site.xml
3.run below command to start MR
yarn hdp-mapreduce-0.0.1-SNAPSHOT.jar com.mapreduce.samples.hive.mr.HiveDriver in_db in_table out_db out_table reduce_count
yarn hdp-mapreduce-0.0.1-SNAPSHOT.jar com.mapreduce.samples.hive.map.HiveMapDriver in_db in_table out_db out_table hbase_table_db hbase_table_name col_family
yarn hdp-mapreduce-0.0.1-SNAPSHOT.jar com.mapreduce.samples.hbase.HbaseDriver in_hbase_db in_hbase_table out_hbase_db out_hbase_table col_family startRow(O) endRow(O)

O---> optional

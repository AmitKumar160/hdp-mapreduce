package com.mapreduce.samples.hbase.mr;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @author Amit kumar
 * @Date: 26/06/2018
 */
public class HbaseDriver extends Configured implements Tool{

	public static Logger logger = LoggerFactory.getLogger(HbaseDriver.class.getName());

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws IOException,InterruptedException,ClassNotFoundException {

		// define configuration and get arguments 
		String jobName="Hbase MapReduce Job";
		Configuration conf=getConf();
		int status=2;
		String startRow=null,endRow=null;
		System.out.println(Arrays.toString(args));
		String inputTableDB=args[0].trim();
		String inputTableName=args[1].trim();
		String outputTableDB=args[2].trim();
		String outputTableName=args[3].trim();
		String colFamily=args[4].trim();
		if(args.length>5){
			startRow=args[5].trim();
			endRow=args[6].trim();
		}
		try{
			conf.set("inputDB", inputTableDB);
			conf.set("inputTable", inputTableName);
			conf.set("outputTable", outputTableName);
			conf.set("colFamily", colFamily);

			Scan scan = new Scan();
			if(startRow!=null && endRow!=null){
				scan.setStartRow(Bytes.toBytes(startRow));
				scan.setStopRow(Bytes.toBytes(endRow));
			}
			scan.setCaching(500);
			scan.setCacheBlocks(false);

			Configuration hbaseConf = HBaseConfiguration.create(conf);
			hbaseConf.set("hbase.client.retries.number", "3");
			hbaseConf.set("hbase.client.pause", "1000");
			hbaseConf.set("zookeeper.recovery.retry", "1");
			// define Job
			Job job = Job.getInstance(hbaseConf, jobName);
			job.setJarByClass(HbaseDriver.class);
			TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(inputTableDB+":"+inputTableName), scan, 
					HbaseMapper.class, ImmutableBytesWritable.class, Put.class, job);
			job.setCombinerClass(HbaseCombiner.class);
			job.setReducerClass(HbaseReducer.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(Put.class);
			job.setOutputFormatClass(HFileOutputFormat2.class);

			Path dir = new Path("/HADOOP/MAPREDUCE/HBASE");
			FileSystem hdfs = FileSystem.get(conf);

			// delete existing directory
			if (hdfs.exists(dir)) {
				hdfs.delete(dir, true);
			}else{
				hdfs.create(dir);
			}
			Connection connection= ConnectionFactory.createConnection(hbaseConf);
			Admin hbase_Admin = connection.getAdmin();
			System.out.println( "Connecting to Hbase" );
			if(hbase_Admin.isTableAvailable(TableName.valueOf(outputTableDB+":"+outputTableName))){

				System.out.println( "Hbase table already exists" );
				System.out.println( "truncating and disable hbase table" );
				hbase_Admin.truncateTable(TableName.valueOf(outputTableDB+":"+outputTableName), false);
				hbase_Admin.disableTable(TableName.valueOf(outputTableDB+":"+outputTableName));
			}else{
				HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(outputTableDB+":"+outputTableName)); 
				htable.addFamily( new HColumnDescriptor(colFamily));
				System.out.println( "Creating hbase table" );
				hbase_Admin.createTable(htable);
				System.out.println("Hbase Table created");
			}
			/*Table table = null;
			try (Connection conn = ConnectionFactory.createConnection(hbaseConf);){
				Admin admin = conn.getAdmin();
				table = conn.getTable(TableName.valueOf(outputTableDB+":"+outputTableName));
				RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(outputTableDB+":"+outputTableName));
				HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
				if (!job.waitForCompletion(true)) {
					return 1;
				}

				LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
				loader.doBulkLoad(dir, (Htable) table);
				FileSystem.get(conf).delete(dir, true);
				return 0;
			} finally {
				table.close();
			}*/


			status=job.waitForCompletion(true)?0:1;
			if(status==0){
				logger.info("Job success with status code:"+status);
			}else{
				logger.info("Job failed with status code:"+status);
			}

		}catch(Exception e){
			e.printStackTrace();
		}
		return status;
	}

	public static void main(String args[]){
		int exit;
		try {
			java.util.Date start_Date = new java.util.Date();
			String str_Dtm = new Timestamp(start_Date.getTime()).toString();

			exit = ToolRunner.run(new HbaseDriver(), args);

			java.util.Date end_Date= new java.util.Date();
			String end_Dtm= new Timestamp(end_Date.getTime()).toString();

			logger.info("Start Time:"+str_Dtm);
			logger.info("End Time:"+end_Dtm);
			logger.info("status code:"+exit);

		}catch(Exception ex){
			ex.printStackTrace();
		}

	}
}

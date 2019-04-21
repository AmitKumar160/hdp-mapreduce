package com.mapreduce.samples.hive.hbase;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapreduce.samples.hive.map.HiveMapDriver;

/**
 * @author Amit kumar
 * @Date: 26/06/2018
 */
public class HiveHbaseDriver extends Configured implements Tool{

	public static Logger logger = LoggerFactory.getLogger(HiveHbaseDriver.class.getName());

	public int run(String[] args) throws IOException,InterruptedException,ClassNotFoundException {

		// define configuration and get arguments 
		String jobName="Hbase MapReduce Job";
		Configuration conf=getConf();
		int status=2;
		System.out.println(Arrays.toString(args));
		String inputTableDB=args[0].trim();
		String inputTableName=args[1].trim();
		String outputTableDB=args[2].trim();
		String outputTableName=args[3].trim();
		String colFamily=args[4].trim();
		try{
			conf.set("inputDB", inputTableDB);
			conf.set("outputTableDB", outputTableDB);
			conf.set("inputTable", inputTableName);
			conf.set("outputTable", outputTableName);
			conf.set("colFamily", colFamily);

			Configuration hbaseConf = HBaseConfiguration.create(conf);
			hbaseConf.set("hbase.client.retries.number", "3");
			hbaseConf.set("hbase.client.pause", "1000");
			hbaseConf.set("zookeeper.recovery.retry", "1");
			// define Job
			Job job = Job.getInstance(hbaseConf, jobName);
			job.setJarByClass(HiveHbaseDriver.class);
			job.setMapperClass(HiveHbaseMapper.class);
			job.setReducerClass(HiveHbaseReducer.class);
			job.setInputFormatClass(HCatInputFormat.class);
			HCatInputFormat.setInput(job, inputTableDB, inputTableName);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(NullWritable.class);
		
			
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
			
			exit = ToolRunner.run(new HiveMapDriver(), args);
			
			java.util.Date end_Date= new java.util.Date();
			String end_Dtm= new Timestamp(end_Date.getTime()).toString();
			
			logger.info("Start Time:"+str_Dtm);
			logger.info("End Time:"+end_Dtm);
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
	}
}

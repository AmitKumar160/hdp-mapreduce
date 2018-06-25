package com.mapreduce.samples.hive;

/**
 * @author Amit kumar
 * @date 25/06/2018
 * @class HiveDriver
 * description : Reading data from hive table and writing to other hive table
 */

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveDriver extends Configured implements Tool {
	
	private static Logger logger = LoggerFactory.getLogger(HiveDriver.class.getName());
	
	public int run(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
		
		// define configuration and get arguments 
		String jobName="Hive MapReduce Job";
		Configuration conf=getConf();
		int status=2;
		System.out.println(Arrays.toString(args));
		String inputTableDB=args[0].trim();
		String inputTableName=args[1].trim();
		String outputTableDB=args[2].trim();
		String outputTableName=args[3].trim();
		String reducerCnt=args[4].trim();
		
		try{
			conf.set("inputDB", inputTableDB);
			conf.set("inputTable", inputTableName);
			conf.set("outputDB", outputTableDB);
			conf.set("outputTable", outputTableName);
			
			// define Job
			Job job = Job.getInstance(conf, jobName);
			
			//set classes like Main/Mapper/Reducer
			//Driver and Mapper is must depending on job types
			job.setJarByClass(HiveDriver.class);
			job.setMapperClass(HiveMapper.class);
			job.setReducerClass(HiveReducer.class);
			
			//setting reducer counts
			job.setNumReduceTasks(Integer.parseInt(reducerCnt));
			
			//setting input/output FORMAT and tables
			job.setInputFormatClass(HCatInputFormat.class);
			HCatInputFormat.setInput(job, inputTableDB, inputTableName);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(DefaultHCatRecord.class);
			
			job.setOutputFormatClass(HCatOutputFormat.class);
			OutputJobInfo output_job_info = OutputJobInfo.create(outputTableDB, outputTableName, null); // keep partitioned value as null if o/p is not partitioned
			HCatOutputFormat.setOutput(job, output_job_info);
			HCatSchema schema = HCatOutputFormat.getTableSchema(job.getConfiguration());
			HCatOutputFormat.setSchema(job, schema);
			
			/*status = 0 success
			 *status = 1 failed
			 *status = 2 by-default
			 * */
			
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
			
			exit = ToolRunner.run(new HiveDriver(), args);
			
			java.util.Date end_Date= new java.util.Date();
			String end_Dtm= new Timestamp(end_Date.getTime()).toString();
			
			logger.info("Start Time:"+str_Dtm);
			logger.info("End Time:"+end_Dtm);
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		
		
		
	}
}

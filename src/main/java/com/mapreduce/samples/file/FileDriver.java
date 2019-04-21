package com.mapreduce.samples.file;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Amit kumar
 * @Date: 12/01/2019
 */
public class FileDriver extends Configured implements Tool{

	public static Logger logger = LoggerFactory.getLogger(FileDriver.class.getName());

	public int run(String[] args) throws IOException,InterruptedException,ClassNotFoundException {

		// define configuration and get arguments 
		String jobName="File MapReduce Job";
		Configuration conf=getConf();
		int status=2;
		System.out.println(Arrays.toString(args));
		String inPath=args[0].trim();
		String outPath=args[1].trim();
		try{
			conf.set("inputPath", inPath);
			conf.set("outputPath", outPath);
			// define Job
			Job job = Job.getInstance(conf, jobName);
			//deleting path if already exists
			FileSystem fs= FileSystem.get(conf);
			if(fs.exists(new Path(outPath))){
				fs.delete(new Path(outPath), true);
			}
			job.setJarByClass(FileDriver.class);
			job.setMapperClass(FileMapper.class);
			job.setReducerClass(FileReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(FileInputFormat.class);
			job.setOutputFormatClass(FileOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(inPath));
			FileOutputFormat.setOutputPath(job, new Path(outPath));
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
			
			exit = ToolRunner.run(new FileDriver(), args);
			
			java.util.Date end_Date= new java.util.Date();
			String end_Dtm= new Timestamp(end_Date.getTime()).toString();
			
			logger.info("Start Time:"+str_Dtm);
			logger.info("End Time:"+end_Dtm);
			logger.info("exit code"+exit);
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
	}
}

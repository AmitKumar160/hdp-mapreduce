package com.mapreduce.samples.file;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Amit kumar
 * @Date: 12/01/2019
 */

public class FileMapper extends Mapper<LongWritable,Text,Text,Text> {


	private static Logger logger = LoggerFactory.getLogger(FileMapper.class.getName());
	static String SPLITTER=","; 

	public void map(LongWritable key,Text value,Context context) throws 
	InterruptedException,IOException{
		try{
			if(value!=null){
				String line=value.toString();
				String[] data=line.replace(","+"$","").split(SPLITTER,-1);
				String userID=data[0].trim();
				String productID=data[1].trim();
				String action=data[2].trim();
				context.write(new Text(userID+"^"+productID), new Text(action));
			}
		}catch(Exception e){
			logger.info("Error in mapper"+e);
		}

	}
}

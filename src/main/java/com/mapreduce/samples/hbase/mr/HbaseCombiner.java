package com.mapreduce.samples.hbase.mr;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseCombiner extends TableReducer<ImmutableBytesWritable,Put,ImmutableBytesWritable> {

	private static Logger logger = LoggerFactory.getLogger(HbaseCombiner.class.getName());
	
	public void Reduce(ImmutableBytesWritable key,Iterable<Result> record,Context context){
		
		try{
			/*for(Result res:record){
				
				Put put = new Put(Bytes.toBytes(key.toString()));
				context.write(key, put);
			}*/
			
		}catch(Exception e){
			logger.info("Error in reduce method"+e);
			
		}
		
	}
}

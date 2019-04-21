package com.mapreduce.samples.file;


import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Amit kumar
 * @Date: 12/01/2019
 */

public class FileReducer extends Reducer<Text,Text,NullWritable,Text> {

	private static Logger logger = LoggerFactory.getLogger(FileReducer.class.getName());
	static String SPLITTER=",";
	static String CAPSPLITTER="^";
	public void Reduce(Text key,Iterable<Text> value,Context context){

		try{
			if(value!=null){

				/*userID 1235A
				 * productID 123XA
				Actions: browse click AddToCart logout
				userID 1245B
				productID 127A
				Actions: browse click AddToCart purchase logout
				*/
				Iterator<Text> itr=value.iterator();
				int purchase=0;
				while(itr.hasNext()){
					Text record=itr.next();
					if(record!=null){
						String action=record.toString();
						if(action.equalsIgnoreCase("Purchase")){
							purchase++;
						}
					}
				}
			   /*Scenario1:
				*key: 1235A^123XA AddToCart
				*value:AddToCart
				*Scenario2:
				*key:1245B^127A
				*value:AddToCart
				*	   purchase
				*/
				if(purchase==0){
					String[] data=key.toString().replace("^"+"$","").split(CAPSPLITTER,-1);
					String userID=data[0].trim();
					String productID=data[1].trim();
					context.write(null, new Text(userID+SPLITTER+productID));
				}

			}

		}catch(Exception e){
			logger.info("Error in reduce method"+e);

		}

	}

}

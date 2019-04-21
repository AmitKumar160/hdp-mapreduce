package com.mapreduce.samples.hbase.mr;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapreduce.samples.utilities.hbaseUtil;


public class HbaseMapper extends TableMapper<ImmutableBytesWritable,Put> {


	private static Logger logger = LoggerFactory.getLogger(HbaseMapper.class.getName());
	String inputDb=null;
	String inputTable=null;
	String colFamily=null;
	public void setup(Context context){

		try{
			Configuration conf = new Configuration();
			conf=context.getConfiguration();
			inputDb=conf.get("inputDB");
			inputTable=conf.get("inputTable");
			colFamily=conf.get("colFamily");

		}catch(Exception e){
			logger.error("Error while reading context"+e);
		}
	}

	public void map(ImmutableBytesWritable row,Result value,Context context) throws 
	InterruptedException,IOException{

		try{
			if(value!=null){

				HashMap<String,String> hbaseMap = new HashMap<String,String>();
				String rowkey=Bytes.toString(value.getRow());
				String uid = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("uid")));
				String first_Name = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("first_Name")));
				String middle_name = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("middle_name")));
				String last_Name = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("last_Name")));
				String title = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("title")));
				String first_line_address = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("first_line_address")));
				String second_line_address = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("second_line_address")));
				String city = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("city")));
				String state = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("state")));
				String postcode = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("postcode")));
				String country = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("country")));
				String account_balance = Bytes.toString(value.getValue(Bytes.toBytes(colFamily),Bytes.toBytes("account_balance")));

				hbaseMap.put("uid", uid);
				hbaseMap.put("first_Name", first_Name);
				hbaseMap.put("middle_name", middle_name);
				hbaseMap.put("last_Name", last_Name);
				hbaseMap.put("title", title);
				hbaseMap.put("first_line_address", first_line_address);
				hbaseMap.put("second_line_address", second_line_address);
				hbaseMap.put("city", city);
				hbaseMap.put("state", state);
				hbaseMap.put("postcode", postcode);
				hbaseMap.put("country", country);
				hbaseMap.put("account_balance", account_balance);

				hbaseUtil hbaseutil = new hbaseUtil();
				Put put = hbaseutil.putData(colFamily, hbaseMap, rowkey);
				ImmutableBytesWritable rky=new ImmutableBytesWritable(Bytes.toBytes(rowkey));
				context.write(rky, put);

			}
		}catch(Exception e){
			logger.info("Error in mapper"+e);
		}

	}
}

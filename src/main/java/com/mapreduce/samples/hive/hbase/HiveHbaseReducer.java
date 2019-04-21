package com.mapreduce.samples.hive.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapreduce.samples.utilities.AppConstants;
import com.mapreduce.samples.utilities.hbaseUtil;

public class HiveHbaseReducer extends Reducer<Text,Text,NullWritable,NullWritable> {

	private static Logger logger = LoggerFactory.getLogger(HiveHbaseReducer.class.getName());
	String inputDb=null;
	String inputTable=null;
	String colFamily=null;
	Connection connection=null;
	Table table=null;
	String outputTableDB=null;
	String outputTableName=null;
	public void setup(Context context){

		try{
			Configuration conf = new Configuration();
			conf=context.getConfiguration();
			inputDb=conf.get("inputDB");
			inputTable=conf.get("inputTable");
			outputTableDB=conf.get("outputTableDB");
			outputTableName=conf.get("outputTable");
			colFamily=conf.get("colFamily");
			Configuration hbaseConf = HBaseConfiguration.create(conf);
			hbaseConf.set("hbase.client.retries.number", "3");
			hbaseConf.set("hbase.client.pause", "1000");
			hbaseConf.set("zookeeper.recovery.retry", "1");
			connection = ConnectionFactory.createConnection(hbaseConf);

		}catch(Exception e){
			logger.error("Error while reading context"+e);
		}
	}
	protected void cleanUp(Context context) throws IOException{
		connection.close();
		table.close();
		cleanUp(context);
	}

	public void Reduce(Text key,Iterable<Text> value,Context context){

		try{
			if(value!=null){
				HashMap<String,String> hbaseMap = new HashMap<String,String>();
				Iterator<Text> itr=value.iterator();

				String uid = key.toString().trim();
				String first_Name = "";
				String middle_name = "";
				String last_Name = "";
				String title = "";
				String first_line_address = "";
				String second_line_address = "";
				String city = "";
				String state = "";
				String postcode = "";
				String country = "";
				String account_balance = "";
				String dob="";
				String last_update="";
				TreeMap<Integer,Double> amount = new TreeMap<Integer,Double>();
				int i=1;
				while(itr.hasNext()){
					Text record = itr.next();
					if(record!=null){

						String val[]=record.toString().replace("^"+"$", "").split(AppConstants.MAP_SPLITTER,-1);
						if(val.length==13){
							first_Name = val[0];
							middle_name = val[1];
							last_Name = val[2];
							title = val[3];
							first_line_address = val[4];
							second_line_address = val[5];
							city = val[6];
							state = val[7];
							postcode = val[8];
							country = val[9];
							account_balance = val[10];
							dob=val[11];
							last_update=val[12];
						}
						amount.put(i, Double.parseDouble(account_balance));
						i++;
					}
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
					hbaseMap.put("account_balance", amount.lastKey().toString());
					hbaseMap.put("DOB", dob);
					hbaseMap.put("last_updated", last_update);


					String rowkey=uid;
					hbaseUtil hbaseutil = new hbaseUtil();
					Put put = hbaseutil.putData(colFamily, hbaseMap, rowkey);
					hbaseMap.clear();
					table = connection.getTable(TableName.valueOf(outputTableDB+":"+outputTableName));
					table.put(put);
				}
			}

		}catch(Exception e){
			logger.info("Error in reduce method"+e);

		}

	}

}

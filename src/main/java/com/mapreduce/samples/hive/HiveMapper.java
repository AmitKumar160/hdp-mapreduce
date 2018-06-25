package com.mapreduce.samples.hive;

import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapreduce.samples.utilities.AppConstants;


/**
 * @author Amit kumar
 * @date: 25/06/2018
 * class : HiveMapper
 */

public class HiveMapper extends Mapper<WritableComparable, DefaultHCatRecord,Text,Text>{

	private static Logger logger = LoggerFactory.getLogger(HiveMapper.class.getName());

	HCatSchema schema = null;
	String inputDb=null;
	String inputTable=null;

	//setup class is efficient to create hbase connections and setting configurations
	//based on requirements
	public void setup(Context context){

		try{
			Configuration conf = new Configuration();
			schema=HCatBaseInputFormat.getTableSchema(context.getConfiguration());
			conf=context.getConfiguration();
			inputDb=conf.get("inputDB");
			inputTable=conf.get("inputTable");

		}catch(IOException e){

			logger.error("Error while reading extracting input Schema"+schema.toString());

		}
	}
	protected void map(WritableComparable key,DefaultHCatRecord value,
			Mapper<WritableComparable, DefaultHCatRecord, Text, Text>.Context context) 
					throws IOException,InterruptedException{

		try{
			if(value!=null){
				
				LinkedHashMap<String,Object> colVal = hiveCols(value);
				//there are 2 ways to read value from hive putting in map as objects or directly using schema both show as below:
				
				//direct using schema and value
				String uid = value.get("unique_id",schema).toString()!=null?value.get("unique_id",schema).toString():"";
				String first_Name = value.get("first_name",schema).toString()!=null?value.get("first_name",schema).toString():"";
				String middle_name = value.get("middle_name",schema).toString()!=null?value.get("middle_name",schema).toString():"";
				String last_Name = value.get("last_name",schema).toString()!=null?value.get("last_name",schema).toString():"";
				String title = value.get("title",schema).toString()!=null?value.get("title",schema).toString():"";
				String first_line_address = value.get("first_line_address",schema).toString()!=null?value.get("first_line_address",schema).toString():"";
				String second_line_address = value.get("second_line_address",schema).toString()!=null?value.get("second_line_address",schema).toString():"";
				String city = value.get("city",schema).toString()!=null?value.get("city",schema).toString():"";
				String state = value.get("state",schema).toString()!=null?value.get("state",schema).toString():"";
				String postcode = value.get("postcode",schema).toString()!=null?value.get("postcode",schema).toString():"";
				String country = value.get("country",schema).toString()!=null?value.get("country",schema).toString():"";
				String account_balance = value.get("account_balance",schema).toString()!=null?value.get("account_balance",schema).toString():"0.00";
				
				//using map
				String dob=String.valueOf(colVal.get("date_of_birth"));
				String last_update=String.valueOf(colVal.get("last_updated_timestamp"));
				
				StringBuffer mapVal= new StringBuffer();
				mapVal.append(first_Name);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(middle_name);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(last_Name);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(title);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(first_line_address);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(second_line_address);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(city);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(state);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(postcode);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(country);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(account_balance);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(dob);
				mapVal.append(AppConstants.MAP_DELIMITER);
				mapVal.append(last_update);
				
				//grouping values on basis of keys(UID)
				if(uid!=null&&uid.length()>0){
				context.write(new Text(uid.trim()), new Text(mapVal.toString()));
				}
			}
		}catch(Exception ex){
			logger.info("error in Map method:"+ex);
		}


	}
	public LinkedHashMap<String,Object> hiveCols(DefaultHCatRecord value) throws Exception{

		LinkedHashMap<String,Object> colValMap=new LinkedHashMap<String,Object>();
		int colsize=schema.getFields().size();
		for(int i=0;i<colsize;i++){
			if(AppConstants.TIMESTAMP.equalsIgnoreCase(schema.get(i).getTypeString())){
				if(value.get(i)==null){
					colValMap.put(schema.get(i).getName(), AppConstants.NOT_AVAILABLE);
				}else{
					colValMap.put(schema.get(i).getName(), ((java.sql.Timestamp)value.get(i)).getTime());
				}

			}else if(AppConstants.DATE.equalsIgnoreCase(schema.get(i).getTypeString())){
				if(value.get(i)==null){
					colValMap.put(schema.get(i).getName(), AppConstants.NOT_AVAILABLE);
				}else{
					colValMap.put(schema.get(i).getName(), ((java.sql.Date)value.get(i)).getTime());
				}

			}else{
				colValMap.put(schema.get(i).getName(),value.get(i));
			}

		}

		return colValMap;

	}

}

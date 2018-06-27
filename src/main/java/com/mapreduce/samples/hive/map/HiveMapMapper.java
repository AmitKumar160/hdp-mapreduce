package com.mapreduce.samples.hive.map;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapreduce.samples.utilities.AppConstants;
import com.mapreduce.samples.utilities.hiveUtil;


/**
 * @author Amit kumar
 * @date: 25/06/2018
 * class : HiveMapMapper
 */

public class HiveMapMapper extends Mapper<WritableComparable, DefaultHCatRecord,NullWritable,HCatRecord>{

	private static Logger logger = LoggerFactory.getLogger(HiveMapMapper.class.getName());

	HCatSchema schema = null;
	String inputDb=null;
	String inputTable=null;
	String hbaseTableSchema=null;
	String hbaseTableName=null;
	String hbaseTable=null;
	String colFamily=null;
	HashMap<String,String> cty_code = new HashMap<String,String>();

	//setup class is efficient to create hbase connections and setting configurations
	//based on requirements
	public void setup(Context context){

		try{
			Configuration conf = new Configuration();
			schema=HCatBaseInputFormat.getTableSchema(context.getConfiguration());
			conf=context.getConfiguration();
			inputDb=conf.get("inputDB");
			inputTable=conf.get("inputTable");
			colFamily=conf.get("colFamily");
			hbaseTableSchema=conf.get("hbaseTableSchema");
			hbaseTableName=conf.get("hbaseTableName");
			hbaseTable=hbaseTableSchema+AppConstants.HBASE_CONCAT+hbaseTableName;

			Configuration hbaseConf = HBaseConfiguration.create();
			hbaseConf.set("hbase.client.retries.number", "3");
			hbaseConf.set("hbase.client.pause", "1000");
			hbaseConf.set("zookeeper.recovery.retry", "1");
			Connection connection= ConnectionFactory.createConnection(hbaseConf);
			scanlookup(connection,hbaseTable,colFamily);

			//if hbase table is small use scan in setup as it is one time process
			//if hbase table is huge use get in reducer is recommended/ else in map
			//example is having country as row key and country code as value i.e., small table



		}catch(IOException e){

			logger.error("Error while reading extracting input Schema"+schema.toString());

		}
	}

	protected void map(WritableComparable key,DefaultHCatRecord value,
			Mapper<WritableComparable, DefaultHCatRecord, NullWritable, HCatRecord>.Context context) 
					throws IOException,InterruptedException{

		try{
			if(value!=null){

				hiveUtil hiveUtil = new hiveUtil();
				LinkedHashMap<String,Object> colVal = hiveUtil.hiveCols(value,schema);
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
				String country_code="";

				HCatRecord record = new DefaultHCatRecord(15);

				record.set(0, Integer.parseInt(uid));

				HiveVarchar varchr = new HiveVarchar();
				HiveChar chr = new HiveChar();
				chr.setValue(first_Name);
				record.set(1, chr);

				if(middle_name!=null && middle_name.length()>0){
					chr.setValue(middle_name);
					record.set(2, chr);
				}else{
					record.set(2, null);
				}

				chr.setValue(last_Name);
				record.set(3, chr);

				if(title!=null && title.length()>0){
					chr.setValue(title);
					record.set(4, chr);
				}else{
					record.set(4, null);
				}

				if(first_line_address!=null && first_line_address.length()>0){
					varchr.setValue(first_line_address);
					record.set(5, varchr);
				}else{
					record.set(5, null);
				}

				if(second_line_address!=null && second_line_address.length()>0){
					varchr.setValue(second_line_address);
					record.set(6, varchr);
				}else{
					record.set(6, null);
				}
				if(city!=null && city.length()>0){
					chr.setValue(city);
					record.set(7, chr);
				}else{
					record.set(7, null);
				}

				if(state!=null && state.length()>0){
					chr.setValue(state);
					record.set(8, chr);
				}else{
					record.set(8, null);
				}
				if(postcode!=null && postcode.length()>0){
					chr.setValue(postcode);
					record.set(9, chr);
				}else{
					record.set(9, null);
				}

				if(country!=null && country.length()>0){
					chr.setValue(country);
					record.set(10, chr);
				}else{
					record.set(10, null);
				}

				record.set(11,Double.parseDouble(account_balance));
				if(!(AppConstants.NOT_AVAILABLE).equalsIgnoreCase(dob)){
					record.set(12, new java.sql.Date(Long.parseLong(dob)));
				}else{
					record.set(12, null);
				}
				if(!(AppConstants.NOT_AVAILABLE).equalsIgnoreCase(last_update)){
					record.set(13, new java.sql.Timestamp(Long.parseLong(last_update)));
				}else{
					record.set(13, null);
				}
				country_code=cty_code.get(country);
				if(country_code!=null && country_code.length()>0){
					chr.setValue(country_code);
					record.set(14, chr);
				}else{
					record.set(14, null);
				}
				context.write(NullWritable.get(), record);

			}
		}catch(Exception ex){
			logger.info("error in Map method:"+ex);
		}


	}

	public void scanlookup(Connection connection,
			String hbaseTable, String colFamily) throws IOException {

		try{
			Table table=connection.getTable(TableName.valueOf(hbaseTable));
			Scan scan = new Scan();
			ResultScanner res = table.getScanner(scan);
			for(Result result=res.next();result!=null;result=res.next()){

				String country=Bytes.toString(result.getRow());
				String country_code=Bytes.toString(result.getValue(Bytes.toBytes(colFamily), 
						Bytes.toBytes("country_code")));
				cty_code.put(country, country_code);
			}

		}catch(Exception e){
			logger.info("error in connecting table"+e);
		}
	}

}

package com.mapreduce.samples;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Amit kumar
 * date:25/06/2018
 */


public class HiveReducer extends Reducer<Text,Text,NullWritable,HCatRecord> {

	public static Logger logger = LoggerFactory.getLogger(HiveReducer.class.getName());
	String TIMESTAMP="timestamp";
	String DATE="date";
	String MAP_DELIMITER="^";
	String MAP_SPLITTER="\\^";
	String NOT_AVAILABLE="N/A";
	HCatSchema schema = null;
	String outputDb=null;
	String outputTable=null;

	public void setup(Context context){

		try{
			Configuration conf = new Configuration();
			conf=context.getConfiguration();
			outputDb=conf.get("outputDB");
			outputTable=conf.get("outputTable");

		}catch(Exception e){

			logger.error("Error in setup Method"+e);

		}
	}
	@Override
	protected void reduce(Text key,Iterable<Text> value,
			Reducer<Text,Text,NullWritable,HCatRecord>.Context context)throws IOException,InterruptedException{

		try{
			if(value!=null){

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

						String val[]=record.toString().replace("^"+"$", "").split(MAP_SPLITTER,-1);
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
				}
				HCatRecord record = new DefaultHCatRecord(14);

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

				record.set(11,amount.lastKey());
				if(!(NOT_AVAILABLE).equalsIgnoreCase(dob)){
					record.set(12, new java.sql.Date(Long.parseLong(dob)));
				}else{
					record.set(12, null);
				}
				if(!(NOT_AVAILABLE).equalsIgnoreCase(last_update)){
					record.set(13, new java.sql.Timestamp(Long.parseLong(last_update)));
				}else{
					record.set(13, null);
				}
				context.write(NullWritable.get(), record);

			}

		}catch(Exception ex){
			logger.info("Error in reduce method"+ex);
		}

	}

}

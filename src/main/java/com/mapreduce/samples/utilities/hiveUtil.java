package com.mapreduce.samples.utilities;

import java.util.LinkedHashMap;

import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * @author Amit kumar
 * @date: 25/06/2018
 */

public class hiveUtil {

	public LinkedHashMap<String,Object> hiveCols(DefaultHCatRecord value,HCatSchema schema) throws Exception{

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

package com.mapreduce.samples.utilities;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Amit kumar
 * @date: 25/06/2018
 */

public class hbaseUtil {

	public Logger logger = LoggerFactory.getLogger(hbaseUtil.class.getName());
	public HashMap<String,String> getData(Connection connection, String tableName, String rowkey, 
			String cf, String Qualifier) throws IOException{

		HashMap<String,String> getMap = new HashMap<String,String>();
		try{
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowkey));
			Result res = table.get(get);
			byte[] val = res.getValue(Bytes.toBytes(cf), Bytes.toBytes(Qualifier));
			String str_Val = Bytes.toString(val);
			getMap.put(rowkey,str_Val);

		}catch(Exception e){
			logger.info("error while getting from Hbase"+e);
		}

		return getMap;

	}

	public HashMap<String,HashMap<String,String>> scanData(Connection connection, String tableName,
			String cf, String filter) throws IOException{

		HashMap<String,HashMap<String,String>> scanMap = new HashMap<String,HashMap<String,String>>();
		HashMap<String,String> map=null;
		try{
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan sc = new Scan();
			ResultScanner scanner = table.getScanner(sc);
			for(Result res:scanner){
				byte [] rowkey = res.getRow();
				map = new HashMap<String,String>();
				List<Cell> cells = res.listCells();
				for(Cell cell: cells){
					String value=Bytes.toString(CellUtil.cloneValue(cell));
					String qualifier=Bytes.toString(CellUtil.cloneQualifier(cell));
					map.put(qualifier, value);
				}
				scanMap.put(Bytes.toString(rowkey), map);
			}

		}catch(Exception e){
			logger.info("error while scanning from Hbase"+e);
		}

		return scanMap;

	}

	public void putData(Connection connection, String tableName, 
			String cf, HashMap<String,HashMap<String,String>> map) throws IOException{
		
		try{
			Table table= connection.getTable(TableName.valueOf(tableName));
			for(Map.Entry<String,HashMap<String,String>> entry: map.entrySet()){
				String rowkey=entry.getKey();
				HashMap<String,String> innerMap=entry.getValue();
				for(Map.Entry<String, String> ent:innerMap.entrySet()){
					String Qualifier=ent.getKey();
					String Value=ent.getValue();
					Put put = new Put(Bytes.toBytes(rowkey));
					put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(Qualifier), Bytes.toBytes(Value));
					table.put(put);
				}
				
			}
			
		}catch(Exception e){
			logger.info("error while putting data into hbase:"+e);
		}
		
		
	}
}

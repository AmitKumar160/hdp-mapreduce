package com.mapreduce.samples.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class propertyUtils {

	public Map<String, String> loadProperty(String path)
			throws FileNotFoundException, IOException 
			{
				Properties in = new Properties();
				FileInputStream fis = new FileInputStream(new File(path));
				in.load(fis);
				Map<String,String> propMap= new HashMap<String,String>();
				Enumeration keys = in.propertyNames();
					while(keys.hasMoreElements()) {
							String key = (String)keys.nextElement();
							propMap.put(key,in.getProperty(key));
					}
				fis.close();
				return propMap;
			}

}

package com.neusoft.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


/**
 * @author liujin
 *
 * @date 2018-3-29
 */
public class ReadConfig
{

	public static Map<String,String> configMap = new HashMap<String,String>();

	public static String fileName = "resource.properties";
	static
	{
		initial();
	}

	private static void initial()
	{
		Properties properties = new Properties();
		InputStream in = null;
		//		File file = new File("./"+fileName);
		try
		{
//			in = ReadConfig.class.getClassLoader().getResourceAsStream(fileName);
//			System.out.println("获取文件流.");
//			/kafka-generator
			in = ReadConfig.class.getResourceAsStream("/"+fileName);  
			if(null==in){
				System.out.println("in is null");
			}
			BufferedReader inBR = new BufferedReader(new InputStreamReader(in));  
			System.out.println("获取文件流成功.");
			properties.load(inBR);
			System.out.println("加载文件成功.");
			Iterator<Map.Entry<Object, Object>> it = properties.entrySet().iterator();
			while (it.hasNext())
			{
				Map.Entry<Object, Object> entry = it.next();
				Object key = entry.getKey();
				Object value = entry.getValue();
				configMap.put(key.toString(), value.toString());
			}
			
		}
		catch (Exception e)
		{
			System.out.println("获取文件流失败.");
			File file = new File(ReadConfig.class.getResource("/").getPath() + "/" + fileName);
			try
			{
				in = new FileInputStream(file);
				properties.load(in);
				Iterator<Map.Entry<Object, Object>> it = properties.entrySet().iterator();
				while (it.hasNext())
				{
					Map.Entry<Object, Object> entry = it.next();
					Object key = entry.getKey();
					Object value = entry.getValue();
					configMap.put(key.toString(), value.toString());
				}
			}
			catch (Exception e1)
			{
				System.out.println("获取文件流失败.");
				e.printStackTrace();
			}
		}
	}

	private static String getFilePath(String fileName)
	{
		//TODO linux 路径待确认 改善
		String filepath = "/";
		String filepathLinux = "/conf/";
		String file = "";
		String os = System.getProperty("os.name");
		if (os.indexOf("Windows") != -1)
		{
			file = filepath + fileName;
		}
		else
		{
			file = filepathLinux + fileName;
		}
		return file;
	}
}

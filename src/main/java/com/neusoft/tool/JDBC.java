package com.neusoft.tool;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.neusoft.kafka.OggDatas.Pair;
import com.neusoft.kafka.OggDatas.TableRecord;
import com.neusoft.kafka.OggDatas.TableRecord.Builder;

/**
 * @author liujin
 *
 * @date 2018-3-29
 */
public class JDBC {
	private int queryCount = 10;
	ReadConfig readConfig = new ReadConfig();
	Map<String,String> configMap = ReadConfig.configMap;
	Connection conn = null;
	public Connection getConn()
	{
		String driver = configMap.get("jdbc.connection.driver_class");
		String url = configMap.get("jdbc.connection.url");
		String name = configMap.get("jdbc.connection.username");
		String pwd = configMap.get("jdbc.connection.password");
		queryCount = Integer.parseInt(configMap.get("queryCount"));
		try
		{
			Class.forName(driver);
			conn = DriverManager.getConnection(url, name, pwd);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
		}
		return conn;
	}
	
	public void closeConn(){
		if(null!=conn){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public Queue<Builder> getQueueFromDb(String tableName, String sql, Object[] conditions) {
		Queue<TableRecord.Builder> tableRecordQueue = new LinkedList<TableRecord.Builder>();
		Connection conn = getConn();
		PreparedStatement ps = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		DatabaseMetaData dbMeta = null;
		int count = 0;
		String pkCol = "";
		try
		{
			dbMeta = conn.getMetaData(); 		
			ResultSet pkRSet = dbMeta.getPrimaryKeys(null, null, tableName); 
			while( pkRSet.next() ) {
				pkCol+=pkRSet.getObject(4)+",";
			}
			ps = conn.prepareStatement(sql);
			if (conditions != null)
			{
				for (int i = 0; i < conditions.length; i++)
				{
					ps.setObject(i + 1, conditions[i]);
				}
			}
			rs = ps.executeQuery();
			rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
	
			// 输出数据   
			while (rs.next())
			{
				TableRecord.Builder tableRecord = TableRecord.newBuilder();
				tableRecord.setTableName(tableName);
				tableRecord.setOperationType("INSERT");
				
				for (int i = 1; i <= columnCount; i++)
				{
					Pair.Builder pair = Pair.newBuilder();
					pair.setCol(rsmd.getColumnName(i).toLowerCase());
					pair.setVal(rs.getString(i)==null?"":rs.getString(i));
					pair.setType(String.valueOf(rsmd.getColumnType(i))); // 获取指定列的SQL类型，是Types中定义的静态常量
					if(pkCol.contains(rsmd.getColumnName(i).toLowerCase())){//匹配主键
						pair.setIsPk("1");
					}else{
						pair.setIsPk("0");
					}
					
					tableRecord.addAftCols(pair.build());
				}
				tableRecordQueue.add(tableRecord);
				if(count++>queryCount)break;//限制查询条数
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if (rs != null)
				{
					rs.close();
				}
				if (ps != null)
				{
					ps.close();
				}
				if (conn != null)
				{
					conn.close();
				}
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
		return tableRecordQueue;
	}
}

package com.neusoft.kafka;

import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import com.neusoft.kafka.OggDatas.Pair;
import com.neusoft.kafka.OggDatas.TableRecord;
import com.neusoft.kafka.OggDatas.Transaction;
import com.neusoft.tool.CommonUtil;
import com.neusoft.tool.JDBC;
import com.neusoft.tool.ReadConfig;

/**
 * @author liujin
 *
 * @date 2018-3-29
 */
public class BeanFactory {
	ReadConfig readConfig = new ReadConfig();
	Map<String,String> configMap = ReadConfig.configMap;
	JDBC jdbc = new JDBC();
	Random rand = new Random();
	private String timePattern = "yyyy-MM-dd HH:mm:ss.sss";
	private final int offset = 3;
	private static String[] charsArray = {"Apple","Google","FaceBook","SpaceX","Tesla","Amazon","Alibaba","Baidu","Tencent"};
	
	private String[] TableNames = configMap.get("table.names").split(",");
	private List<Queue<TableRecord.Builder>> tableRecordQueues = new ArrayList<Queue<TableRecord.Builder>>();
	
	public BeanFactory(){
		if(KafkaDemoProducer.makeType.equals("db"))
		makeQueues();//根据表数据构造tableRecordQueue
	}
//	public BeanFactory(int count){
//		makeQueues();
//	}
	
	private void makeQueues() {
		String sql  = "";
		for (int i = 0; i < TableNames.length; i++) {
			sql = "select * from "+TableNames[i]+"  E";
			System.out.println(sql);
			Queue<TableRecord.Builder> tableRecordQueue = new LinkedList<TableRecord.Builder>();
			tableRecordQueue = jdbc.getQueueFromDb(TableNames[i],sql, null);
			tableRecordQueues.add(tableRecordQueue);
		}
	}
	
	public Transaction.Builder getTransaction(int commitNo,int recordNumber){
		
		Transaction.Builder transaction = Transaction.newBuilder();
		String ST1_1=CommonUtil.DateToString(new Date(), timePattern);
		transaction.setKafkaCommitTime(ST1_1);
		transaction.setCommitNo(String.valueOf(commitNo));
		transaction.setRecordNumber(String.valueOf(recordNumber));
		transaction.setIsLastSplit("1");
		transaction.setIsSplit("1");
		transaction.setSplitNo("0");
		
		for (int i = 0; i < recordNumber; i++) {
			TableRecord.Builder tableRecord =tableRecordQueues.get(rand.nextInt(27)%tableRecordQueues.size()).poll(); //随机从不同表中选取tableRecord
			if(tableRecord!=null){
				transaction.addRecords(tableRecord.build());
			}
		}
		
		String ST1_2=CommonUtil.DateToString(new Date(), timePattern);
		transaction.setOggCommitTime(ST1_2);
		return transaction;
	}
	
	public Transaction.Builder getTransaction(){
		Transaction.Builder transaction = Transaction.newBuilder();
		String ST1_1=CommonUtil.DateToString(new Date(), timePattern);
		transaction.setKafkaCommitTime(ST1_1);
		transaction.setCommitNo(String.valueOf(new Time(System.currentTimeMillis())));
		int index = rand.nextInt(offset)+1;
		for (int i = 0; i < index; i++) {
			TableRecord.Builder tableRecord =getTableRecord(i+1); 
			transaction.addRecords(tableRecord.build());
		}
		transaction.setRecordNumber(String.valueOf(index));
		transaction.setIsLastSplit("1");
		transaction.setIsSplit("1");
		transaction.setSplitNo("0");
		String ST1_2=CommonUtil.DateToString(new Date(), timePattern);
		transaction.setOggCommitTime(ST1_2);
		
		return transaction;
	}
	
	private TableRecord.Builder getTableRecord(int random){
		TableRecord.Builder tableRecord = TableRecord.newBuilder();
		tableRecord.setTableName(TableNames[rand.nextInt(TableNames.length)]);
		tableRecord.setOperationType("INSERT");
		int index = rand.nextInt(random+offset)+1;
		for (int i = 0; i < index; i++) {
			Pair.Builder pair = getPair(i+1);
			tableRecord.addAftCols(pair.build());
		}
		return tableRecord;
	}

	private Pair.Builder getPair(int index) {
		Pair.Builder pair = Pair.newBuilder();
		pair.setCol("Col"+index);
		pair.setVal(charsArray[rand.nextInt(charsArray.length)]);
		pair.setType(String.valueOf(Types.VARCHAR));
		if(index==1){
			pair.setIsPk("1");
			pair.setVal(charsArray[rand.nextInt(charsArray.length)]+System.currentTimeMillis());
		}else{
			pair.setIsPk("0");
			pair.setVal(charsArray[rand.nextInt(charsArray.length)]);
		}
//		System.out.println(index+"@col::"+	pair.getCol()+";val::"+	pair.getVal());
		return pair;
	}
}

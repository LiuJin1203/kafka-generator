# kafka-generator
模拟OGG向Kafka生产数据（数据库和随机两种方式）

1.kafka-generator工程整体功能说明
	kafka生成器，主要实现将关系型数据库表数据或者通过随机生成器构造成OGG.Transaction对象，
	以二进制流的形式推送到kafka消息队列中，消费者通过相同TOPIC接收消息。
	其中，kafka集群地址、数据库连接、数据库表名、TOPIC等相关参数可通过resource.properties配置文件配置。
	
2.Transaction对象构造设计
 (1)每个Transaction对象需包含多个TableRecord(数量可随机)，每个TableRecord对象包含多个Pair(数量与列对应).
	这里TableRecord相当于表一条记录，Pair对象相当于一行记录中的一列，Pair.Type与java.sql.Type类型数值一致
 (2)每张table对应一个Queue，将查询结果构造出的TableRecord追加到Queue中，将多个tableQueue添加到List中准备好。
 	在构造Transaction对象时，随机从tableQueueList中取不同表的队列tableQueue,使用poll()方法取出并移除且可以返回null
 	如果从队列tableQueue中取出的TableRecord为null,则不添加到Transaction中。
 (3)queryCount控制表查询数量，在配置文件中可配；主函数入口参数有两个，依次代表发送记录条数(正整数)、模拟对象方式(db/rand)。
 
3.OGG数据结构说明
message Pair {
	required string col  = 1;
	required string val  = 2;
	required string type = 3;
	required string isPk = 4;
}
message TableRecord {
	required		 string		table_name		   = 1;
	required		 string		operation_type	   = 2;
	repeated Pair		bef_cols		   = 3;
	repeated Pair     	aft_cols		   = 4;
}
message Transaction {
	required string commit_no		    = 1;
	required string ogg_commit_time		= 2;
	required string kafka_commit_time	= 3;
	required string record_number	    = 4;
	required string is_split			= 5;
	required string split_no			= 6;
	required string is_last_split	    = 7;
	repeated TableRecord records		= 8;
}

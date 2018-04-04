package com.neusoft.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.neusoft.kafka.OggDatas.Transaction;

/**
 * @author liujin
 *
 * @date 2018-3-29
 */
public class KafkaDemoConsumer {
	private final  KafkaConsumer<String, byte[]> consumer;
	public final static String TOPIC = "TEST-TOPIC";
	
	public KafkaDemoConsumer() {
		
		Properties props = new Properties();
		props.put("zookeeper.connect", "s158:2181");
		props.put("bootstrap.servers", "s38:9092,s39:9092,s40:9092,s41:9092");
		 props.put("group.id", "test");
		 props.put("enable.auto.commit", "true");
		 props.put("auto.offset.reset" ,"latest");
		 props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		 props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		 consumer = new KafkaConsumer<>(props);
		 consumer.subscribe(Arrays.asList(TOPIC));
		}
	
	void consume() {
//	     while (true) {
//	         ConsumerRecords<String, String> records = consumer.poll(100);
//	         for (ConsumerRecord<String, String> record : records)
//	             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//	     }
	}
	
	void consumeBean() {
	     while (true) {
	         ConsumerRecords<String, byte[]> records = consumer.poll(100);
	         for (ConsumerRecord<String, byte[]> record : records){
	            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value().toString());
	         	decode(record.value());
	         	System.out.println("=================================");
	         }
	     }
	}
	private void decode(byte[] msg) {
		Transaction data = null;
		try {
			data = OggDatas.Transaction.parseFrom(msg);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	    for (int i=0;i<data.getRecordsCount();i++) {
	    	System.out.println(data.getRecords(i).toString());
	    }
	}

}

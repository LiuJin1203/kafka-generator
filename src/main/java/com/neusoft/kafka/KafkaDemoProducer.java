package com.neusoft.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.neusoft.kafka.OggDatas.Transaction;
import com.neusoft.tool.ReadConfig;

/**
 * @author liujin
 *
 * @date 2018-3-29
 */
public class KafkaDemoProducer {
	ReadConfig readConfig = new ReadConfig();
	Map<String,String> configMap = ReadConfig.configMap;
	
	public static int MessageCOUNT = 100;
	public static String makeType = "random";
	private final Producer<String, byte[]> producer;
    public final  String TOPIC = configMap.get("TOPIC");
	/**
	 * 无参构造方法
	 * <p>初始化kafka-prop,构造producer
	 */
    public KafkaDemoProducer(){
        Properties props = new Properties();
        props.put("zookeeper.connect", configMap.get("zookeeper.connect"));
        props.put("bootstrap.servers", configMap.get("bootstrap.servers"));
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		 
        producer = new KafkaProducer<>(props);
    }

    void produce() {
        int messageNo = 10;
        final int COUNT = 2000;

        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
//            producer.send(new ProducerRecord<String, String>(TOPIC, key ,data));
            System.out.println(data);
            messageNo ++;
        }
        producer.close();
    }

    /**
     * 构造Transaction对象
     * <p>makeType 构造类型 db从表中构造 其他随机生成
     */
    public void produceBean(){
        int messageNo = 1;
        int COUNT = MessageCOUNT;
        BeanFactory beanInstance = new BeanFactory();
        while (messageNo < COUNT) {
        	Transaction.Builder transaction = null;
        	if(makeType.equals("db")){
        		transaction = beanInstance.getTransaction(messageNo,new Random().nextInt(3)+1);
        	}else{
        		transaction = beanInstance.getTransaction();
        	}
	    	Transaction t = transaction.build();
	    			
	    	String key = String.valueOf(messageNo);
	    	System.out.println(t);
	    	messageNo++;
	    	producer.send(new ProducerRecord<String,byte[]>(TOPIC, key ,t.toByteArray()));
	    	System.out.println("====="+messageNo+"=====");
        }
        producer.close();
    }
}

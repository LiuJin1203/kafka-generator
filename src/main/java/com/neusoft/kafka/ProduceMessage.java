package com.neusoft.kafka;

/**
 * @author liujin
 *
 * @date 2018-3-29
 */
public class ProduceMessage {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args!=null&&args.length>1){
			KafkaDemoProducer.MessageCOUNT = Integer.parseInt(args[0]);
			KafkaDemoProducer.makeType = args[1];
		}else if(args!=null&&args.length==1){
			KafkaDemoProducer.MessageCOUNT = Integer.parseInt(args[0]);
		}
//		KafkaDemoProducer.makeType = "db";
//		KafkaDemoProducer.MessageCOUNT = 100;
		
		new KafkaDemoProducer().produceBean();
	}

}

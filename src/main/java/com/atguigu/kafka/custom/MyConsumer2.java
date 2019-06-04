package com.atguigu.kafka.custom;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


/*
 * 1. 重置offset
 * 		    public static final String AUTO_OFFSET_RESET_DOC = 
 * "What to do when there is no initial offset in Kafka or i
 * f the current offset does not exist any more on the server (e.g. because that data has been deleted): 
 * <ul><li>earliest: automatically reset the offset to the earliest offset<li>latest: 
 * automatically reset the offset to the latest offset</li><li>none: 
 * throw exception to the consumer if no previous offset is found for the consumer's group
 * </li><li>anything else: throw exception to the consumer.</li></ul>";
		
		从头消费：  ①有个新的组
		        ②设置ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"

 * 		
 * 2. ConsumerConfig
 * 
 * 3. 自动提交可能出现的问题： 如果提交时间较短，而处理消费的数据时间过长，存在丢失数据的风险！
 * 			解决：手动提交offset!
 * 				enable.auto.commit=false
 * 
 * 4.  consumer.commitSync(); 手动同步提交！
 * 			可能出现的问题： 数据的重复消费！
 * 
 * 
 * 
 * 
 */
public class MyConsumer2 {
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		// 连接的broker的主机名和端口号
	     props.put("bootstrap.servers", "hadoop102:9092");
	     
	     props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	     
	     // 组名
	     props.put("group.id", "test1");
	     // 是否启用自动提交offset
	     props.put("enable.auto.commit", "true");
	     
	     // 手动提交
	     props.put("enable.auto.commit", "false");
	     //  每间隔多久自动提交一次offset
	    // props.put("auto.commit.interval.ms", "1000");
	     // key-value反序列化器
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    
	    
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     
	     
	     
	     // 订阅主题
	     consumer.subscribe(Arrays.asList("hello"));
	     
	     while (true) {
	    	 
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         
	         for (ConsumerRecord<String, String> record : records) {
	        	 
	             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	         
	         }
	         
	        
	     }

		
		
	}

}

package com.atguigu.kafka.custom;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;


/*
  自定义分区和主题
 * 
 *  类似： bin/kafka-console-consumer.sh --bootstrap-server xxx --topic hello --partition 0 --offset 0
 * 
 * 
 */
public class MyConsumer {
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		// 连接的broker的主机名和端口号
	     props.put("bootstrap.servers", "hadoop102:9092");
	     
	     props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	     
	     // 组名
	     props.put("group.id", "test2");
	     // 是否启用自动提交offset
	    // props.put("enable.auto.commit", "true");
	     
	     // 手动提交
	     props.put("enable.auto.commit", "true");
	     //  每间隔多久自动提交一次offset
	     props.put("auto.commit.interval.ms", "1000");
	     // key-value反序列化器
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    
	    
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     
	     // 指定主题和分区
	     TopicPartition topicPartition = new TopicPartition("hello", 0);
	     
	     
	     consumer.assign(Arrays.asList(topicPartition));
	     
	     // 指定位置
	     consumer.seek(topicPartition, 0);
	     
	     while (true) {
	    	 
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         
	         for (ConsumerRecord<String, String> record : records) {
	        	 
	             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	         
	         }
	         
	        
	     }

		
		
	}

}

package com.atguigu.kafka.custom;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TimeInterceptor implements ProducerInterceptor<Integer,String>{

	// 从Producer的配置属性中获取配置
	@Override
	public void configure(Map<String, ?> configs) {
	}

	// 拦截ProducerRecord，处理，返回处理后的ProducerRecord
	@Override
	public ProducerRecord<Integer,String> onSend(ProducerRecord<Integer,String> record) {
		
		String value = (String) record.value();
		
		value=System.currentTimeMillis()+","+value;
		
		return new ProducerRecord<Integer, String>(record.topic(), record.partition(), record.key(), value);
	}

	//收到server的确认消息
	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
	}

	// 在对应的Producer执行close()后，调用
	@Override
	public void close() {
	}

}

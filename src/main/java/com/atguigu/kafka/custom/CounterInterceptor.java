package com.atguigu.kafka.custom;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CounterInterceptor implements ProducerInterceptor<Integer,String>{
	
	private int successcount;
	private int failedcount;

	@Override
	public void configure(Map<String, ?> configs) {
	}

	@Override
	public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
		return record;
	}

	
	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		
		if (metadata !=null) {
			successcount++;
		}else {
			failedcount++;
		}

	}

	@Override
	public void close() {
		
		System.out.println("success:"+successcount);
		System.out.println("failed:"+failedcount);
		
	}

}

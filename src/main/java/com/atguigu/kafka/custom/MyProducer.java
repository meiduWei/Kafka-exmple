package com.atguigu.kafka.custom;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;

// bin/kafka-console-producer.sh  --broker-list xx  --topic xx

/*
 * 1.生成数据流程
 * 		①主线程创建Producer,封装ProducerRecord
 * 			调用Producer.send(ProducerRecord)
 * 
 *      ②经过拦截器链拦截处理
 *      
 *      ③使用key-value序列化器序列化key-value
 *      
 *      ④计算分区号
 *      		a) ProducerRecord已经指定了分区，采用指定的
 *      		b) 没有指定，调用Partitioner.partition()来计算分区
 *      
 *      默认的partitioner通过以下方式指定！
 * 
 * this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
 * 		使用DefaultPartitioner来分区！
 * 
 * 2. ProducerConfig ： 生产者的配置对象，包含了所有生产者可能用到的属性！
 * 
 * 3. 拦截器的赋值：
 * 
 * 		  this.interceptors = interceptorList.isEmpty() ? null : new ProducerInterceptors<>(interceptorList);
 * 
 * 4. 自定义的拦截器必须实现ProducerInterceptor.class
 * 		配置：   ProducerConfig.INTERCEPTOR_CLASSES_CONFIG=List<String> klasses 
 */
public class MyProducer {
	
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		List<String> interceptors = new ArrayList<>();
		
		interceptors.add("com.atguigu.kafka.custom.TimeInterceptor");
		interceptors.add("com.atguigu.kafka.custom.CounterInterceptor");
		
		// 创建一个Properties，声明producer的相关属性信息
		Properties props = new Properties();
		// broker服务的主机名和端口号
		 props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
		 // 配置拦截器链
		 props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
		 // 声明ack的级别
		 props.put("acks", "all");
		 // 重试次数
		 props.put("retries", 3);
		 props.put("name", "myproducer");
		 // 满足一批发送数据的大小
		 props.put("batch.size", 16384);
		 // 发送数据间隔
		 props.put("linger.ms", 1000);
		 // buffer大小
		 props.put("buffer.memory", 33554432);
		 // 设置自定义分区组件
		 props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.kafka.custom.MyPartitioner");
		 
		 
		 // key-value的序列化器
		 // 根据自己的record的key-value类型选择何时的序列化器
		 props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		
		// 创建一个Producer对象
		KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);
		 
		// send返回Future类型，Future.get()可以使当前线程阻塞，来获取Future执行的结果，必须等待Future线程执行完毕才会向下执行
		for (int i = 0; i < 6; i++) {
			
			kafkaProducer.send(new ProducerRecord<Integer, String>("hello", null, i, "atguigu:"+i),new Callback() {
				
				//  消息已经成功发送的回调方法
				// 如果消息已经被server 确认收到，它会为当前消息创建一个 RecordMetadata对象
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					
					if (exception == null) {
						
						System.out.println("Topic:"+metadata.topic()+"--->"+metadata.partition()+"--->"+metadata.offset());
						
					}
					
				}
			}).get();
			
		}
		
		// 执行和当前的producer相关的其他组件的close()
		kafkaProducer.close();
		 
	}
	

}

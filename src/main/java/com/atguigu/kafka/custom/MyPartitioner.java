package com.atguigu.kafka.custom;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class MyPartitioner  implements Partitioner {

	// 从当前producer的配置中获取参数
	@Override
	public void configure(Map<String, ?> configs) {
		
		System.out.println(configs.get("name"));
	}

	//计算分区号  ，返回int型的值，不能超过当前topic的总的分区数
	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		
		  List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
	       int numPartitions = partitions.size();
		
		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

	// 当producer执行close()时，调用MyPartitioner的close()
	@Override
	public void close() {
	}

}

package vn.com.vndirect.testkafka;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class SimpleConsumer {
	
	ConsumerConnector consumerConnector;
	
	public static void main(String[] argv) throws UnsupportedEncodingException {
		SimpleConsumer consumer = new SimpleConsumer();
		consumer.run();
    }
 
    public SimpleConsumer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect","10.26.0.166:2181");
        properties.put("group.id","test-group5");
        properties.put("auto.offset.reset","largest");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }
    

	public void run() {
		System.out.println("Start running");
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("page_visits_test4", new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get("page_visits_test4").get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		int i = 0;
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> messageAndMetaData = it.next();
			System.out.println("Receiving message: " + new String(messageAndMetaData.message()) + " with offset: " + messageAndMetaData.offset() + " in partition: " + messageAndMetaData.partition());
		}
		
		System.out.println("End running");
	}
}

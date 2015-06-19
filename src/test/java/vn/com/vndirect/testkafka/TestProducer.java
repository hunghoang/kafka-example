package vn.com.vndirect.testkafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {

	Producer<String, String> producer;
	
	TestProducer() {
		Properties props = new Properties();
		props.put("metadata.broker.list", "10.26.0.166:9092");
		props.put("compression.codec", "gzip");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "vn.com.vndirect.testkafka.SimplePartitioner");
		props.put("producer.type", "async");
		props.put("request.required.acks", "0");
		props.put("batch.num.messages", "5000");
		
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}
	
	void send(String string) {
		KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits_test4", "key.8", string);
		producer.send(data);
	}
    void close() {
    	if (producer != null) producer.close();
    }
    
	public static void main(String[] args) {
		TestProducer testProducer = new TestProducer();
		int numberMessage = 100000;
		System.out.println("Start sending data");
		String data = "type = AccountMast, data={ADVANCELINE=0, AVLLIMIT=0, RECEIVING=0, ACTYPE=4031, ADDAMT=0, DFDEBT=0, MRIRATE=100, ACCT_RTT=10000, DFRTT=10000000, RN=1, AUTOADVANCE=N, STATUS=A, MRCRLIMITMAX=100000000000, ACCOUNT=0001003689, DEPOFEEACR=0, OUTSTANDING=0, PPTN_ACCEPT=0, MAXNSADVAMT=0, MAXADVPAYMENTAMT=0, AVLTDAMT=0, PP_MRONLY=0, TRADEPHONE=2222, MARGINRTT=10000000, DFDEBAMT=0, ODAMT=0, TNODAMT=0, BALANCE=0, BALDEFOVD=0, TOTALDEBT=0, DEAL_ADV=0, CUSTODYCD=021C003689, TNTYPE=null, MAXADVPAYMENTDFAMT=0}";
		//String data = "type = AccountMast, data={ADVANCELINE=0, AVLLIMIT=0, RECEIVING=0, ACTYPE=4031, ADDAMT=0, DFDEBT=0, MRIRATE=100, ACCT_RTT=10000, DFRTT=10000000, RN=1, AUTOADVANCE=N, STATUS=A, MRCRLIMITMAX=100000000000, ACCOUNT=0001003689, DEPOFEEACR=0, OUTSTANDING=0, PPTN_ACCEPT=0, MAXNSADVAMT=0, MAXADVPAYMENTAMT=0, AVLTDAMT=0, PP_MRONLY=0, TRADEPHONE=2222, MARGINRTT=10000000, DFDEBAMT=0, ODAMT=0, TNODAMT=0, BALANCE=0, BALDEFOVD=0, TOTALDEBT=0";
		long start = System.currentTimeMillis();
		for(int i = 0; i< numberMessage; i++) {
			testProducer.send(data + i);
		}
		long last = System.currentTimeMillis() - start;
		System.out.println("Total spending time: " + last);
		System.out.println("Publish rate: " + numberMessage / (last / 1000.0) + " messages/s");
		testProducer.close();
	}
}

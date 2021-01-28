package com.cloudera.flink.example;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class FlinkKafkaSimpleStream {

	public static void main(String[] args) throws Exception {
		
		String brokers = args[0];
		String topic = args[1];
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", brokers);
		properties.setProperty("group.id", "flink-test");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.mechanism", "PLAIN");
		properties.setProperty("sasl.kerberos.service.name", "kafka");
		
		properties.setProperty("ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts");
		properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"my_user\" password=\"*******\";");


		FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(topic,
				new SimpleStringSchema(), properties);
		myConsumer.setStartFromEarliest();

		DataStream<String> stream = env.addSource(myConsumer);
		stream.print();
		env.execute();

	}

}

package com.cloudera.flink.example;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import com.cloudera.flink.bean.CovidBean;
import com.cloudera.flink.filter.DedupeFilterCustomState;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FlinkKafkaDeDupCustomStream {

	private static final Gson gson = new GsonBuilder().create();
	private static long DEDUPE_CACHE_EXPIRATION_TIME_MS = 10000;

	public static void main(String[] args) throws Exception {
		
		String brokers = args[0];
		String topic = args[1];
		
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(2000);
		
		System.setProperty("java.security.auth.login.config", "./src/main/resources/jaas.conf");
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", brokers);
		properties.setProperty("group.id", "flink-test");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.mechanism", "PLAIN");
		properties.setProperty("sasl.kerberos.service.name", "kafka");
		properties.setProperty("ssl.truststore.location", "./src/main/resources/truststore.jks");

		FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(topic,
				new SimpleStringSchema(), properties);
		myConsumer.setStartFromEarliest();

		DataStream<String> stream = env.addSource(myConsumer);

		DataStream<CovidBean> messageStream = stream.map(json -> gson.fromJson(json, CovidBean.class))
				.keyBy(key -> key.getLocation() + key.getCountry_code())
				.filter(new DedupeFilterCustomState<>(key -> key.getLocation() + key.getCountry_code(), DEDUPE_CACHE_EXPIRATION_TIME_MS));
		messageStream.print();
		env.execute();

	}

}

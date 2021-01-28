package com.cloudera.flink.example;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import com.cloudera.flink.bean.CovidBean;
import com.cloudera.flink.filter.DedupeFilterValueState;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FlinkKafkaDeDupStateStream {

	private static final Gson gson = new GsonBuilder().create();

	public static void main(String[] args) throws Exception {
		
		String brokers = args[0];
		String topic = args[1];
		
		//For local execution enable this property
		//System.setProperty("java.security.auth.login.config", "./src/main/resources/jaas.conf");		
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(2000);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", brokers);
		properties.setProperty("group.id", "flink-test");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.mechanism", "PLAIN");
		properties.setProperty("sasl.kerberos.service.name", "kafka");
		
		//For local execution enable this property
		//properties.setProperty("ssl.truststore.location", "./src/main/resources/truststore.jks");
		
		//Disable these properties while running on local
		properties.setProperty("ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts");
		properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"my_user\" password=\"*******\";");


		FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(topic,
				new SimpleStringSchema(), properties);
		myConsumer.setStartFromEarliest();

		DataStream<String> stream = env.addSource(myConsumer);

		DataStream<CovidBean> messageStream = stream.map(json -> gson.fromJson(json, CovidBean.class))
				.keyBy(key -> key.getLocation() + key.getCountry_code())
				.filter(new DedupeFilterValueState<CovidBean>());
		messageStream.print();
		env.execute();

	}

}

package com.cloudera.flink.scala.example

import java.util.Properties

import org.apache.flink.streaming.api.scala.extensions._

import com.cloudera.flink.scala.bean.CovidBean
import com.cloudera.flink.scala.filter.DedupeFilterValueState

import com.google.gson.GsonBuilder
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._

object FlinkKafkaDeDupStateStreamLocal {
  
  val gson = new GsonBuilder().create()
  
  def main(args: Array[String]): Unit = {

    val brokers = args(0)
    val topic = args(1)
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(2000)

    System.setProperty("java.security.auth.login.config", "./src/main/resources/jaas.conf")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", brokers)
    properties.setProperty("group.id", "flink-test")
    properties.setProperty("security.protocol", "SASL_SSL")
    properties.setProperty("sasl.mechanism", "PLAIN")
    properties.setProperty("sasl.kerberos.service.name", "kafka")
    properties.setProperty("ssl.truststore.location",
      "./src/main/resources/truststore.jks")

    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    myConsumer.setStartFromEarliest()
    
    val stream = env.addSource(myConsumer)

    val messageStream = stream
      .map[CovidBean]((json:String) => gson.fromJson(json, classOf[CovidBean]))
      .keyBy((key: CovidBean) => key.location + key.country_code)
      .filter(new DedupeFilterValueState[CovidBean]())
    
    messageStream.print()
    env.execute()
  }
}

package org.apache.flink.quickstart.kafka

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

object ClickEventModificationJob2 extends App {

  val kafkaConsumerProps = new Properties()
  kafkaConsumerProps.put("bootstrap.servers", "localhost:9092")
  kafkaConsumerProps.put("group.id", "test-group2")
  kafkaConsumerProps.put("flink.poll-timeout", "10")
  kafkaConsumerProps.put("max.poll.records", "50")

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("client.id", "test-client22")


  val env = StreamExecutionEnvironment.createLocalEnvironment(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(8)
  env.getConfig.disableSysoutLogging()

  val outStream: DataStream[ClickEvent] =
    env.addSource(new FlinkKafkaConsumer010[ClickEvent]("TEST", new ClickEventOps("TEST"), kafkaConsumerProps)).rebalance



  outStream
    .filter(_.status == PreprocessingEventStatus)
    .map(c => {
      println(c);
      c.copy(status = FeatureScoringEventStatus)
    })
    .addSink(new FlinkKafkaProducer010[ClickEvent]("BusinessEvents", new ClickEventOps("BusinessEvents"), kafkaProps))

  env.execute("SDP ClickEvent Modification App 2")
}

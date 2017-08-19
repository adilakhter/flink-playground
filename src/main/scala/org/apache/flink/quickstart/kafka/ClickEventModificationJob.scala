package org.apache.flink.quickstart.kafka

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import scala.collection.JavaConversions._

object ClickEventModificationJob extends App {

  val kafkaConsumerProps = new Properties()
  kafkaConsumerProps.put("bootstrap.servers", "localhost:9092")
  kafkaConsumerProps.put("group.id", "test-group3")
  kafkaConsumerProps.put("flink.poll-timeout", "10")
  kafkaConsumerProps.put("max.poll.records", "50")

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("client.id", "test-client21")


  val env = StreamExecutionEnvironment.createLocalEnvironment(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(8)
  env.getConfig.disableSysoutLogging()

  val outStream: DataStream[ClickEvent] =
    env.addSource(new FlinkKafkaConsumer010[ClickEvent]("BusinessEvents", new ClickEventOps("BusinessEvents"), kafkaConsumerProps)).rebalance



  outStream
    .filter(_.status == FeatureScoringEventStatus)
    .map(c => {
      println(c);
      c.copy(status = ProcessingDoneEventStatus)
    })
    .addSink(new FlinkKafkaProducer010[ClickEvent]("ScoredEvents", new ClickEventOps("ScoredEvents"), kafkaProps))


  env.execute("SDP ClickEvent Modification App 2 ")
}

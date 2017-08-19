package org.apache.flink.quickstart.kafka

import grizzled.slf4j.Logging
import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

object StreamKafkaDemo extends Logging with App {

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("client.id", "test-client1")

  val kafkaConsumerProps = new Properties()
  kafkaConsumerProps.put("bootstrap.servers", "localhost:9092")
  kafkaConsumerProps.put("group.id", "test-group1")
  kafkaConsumerProps.put("flink.poll-timeout", "10")
  kafkaConsumerProps .put("max.poll.records", "50")

  val parallelism = 8
  val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(parallelism)
  env.getConfig.disableSysoutLogging()


  val inStream: DataStream[ClickEvent] = env.fromCollection(getAsSeq(System.currentTimeMillis())).name("input")
  val outStream: DataStream[ClickEvent] = env.addSource(new FlinkKafkaConsumer010[ClickEvent](Seq("BusinessEvents", "ScoredEvents") , new ClickEventOps("BusinessEvents"), kafkaConsumerProps)).rebalance

  val connectedStreams = inStream.keyBy(ce ⇒ ce.id).connect(outStream.keyBy(ce ⇒ ce.id))

  val kafkaStreams = connectedStreams.flatMap(new CoFlatMapFunction[ClickEvent, ClickEvent, ClickEvent] {
    var messages = Map.empty[String, ClickEvent]
    override def flatMap1(value: ClickEvent, out: Collector[ClickEvent]): Unit = {
      messages += (value.id -> value)

      println("++no of values: "+ messages.values.size)

      // TODO:
      // Send inflight HealthStatus status
      // Update log
      // send it to kafka
      out.collect(value)
    }

    override def flatMap2(value: ClickEvent, out: Collector[ClickEvent]): Unit = {
      value.status match {
        case FeatureScoringEventStatus ⇒
          messages -= value.id
        case ProcessingDoneEventStatus ⇒
          messages -= value.id
        case _ ⇒
          // Do nothing
      }

      println("--no of values: "+ messages.values.size)
    }
  }).setParallelism(1).name("ProcessStreams")


  kafkaStreams.filter(_.status == PreprocessingEventStatus).addSink(new FlinkKafkaProducer010[ClickEvent]("TEST", new ClickEventOps("TEST"), kafkaProps))


  env.execute("SDP Heartbeat App")
}

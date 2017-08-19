package org.apache.flink.quickstart

import org.apache.flink.quickstart.kafka.TickSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object FlinkTickSourceDemo extends App {

  val env = StreamExecutionEnvironment.createLocalEnvironment(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
  env.setParallelism(1)

  val source1 = new TickSource((rand) ⇒ rand.nextDouble(), 1000, 5000)
  val firstInput = env.addSource(source1)

  firstInput.print()

  env.execute()
}

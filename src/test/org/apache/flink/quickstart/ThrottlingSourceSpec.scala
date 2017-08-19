package org.apache.flink.quickstart

import org.apache.flink.streaming.api.operators.{StreamOperator, StreamSource}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

class ThrottlingSourceSpec extends App {


  val env = StreamExecutionEnvironment.createLocalEnvironment(1)
  env.setParallelism(1)

  val s: StreamSource[Int, ThrottlingSource[Int]] = new StreamSource[Int, ThrottlingSource[Int]](new ThrottlingSource[Int]((1 to 10).toIterator, 1))


}

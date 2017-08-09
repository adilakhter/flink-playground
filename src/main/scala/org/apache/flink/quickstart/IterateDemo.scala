package org.apache.flink.quickstart

import org.apache.flink.quickstart.IterateDemo.{env, iteratingSource}
import org.apache.flink.streaming.api.scala._


object IterateDemo extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  val seq = 1.to(1000)
  val source = env.fromCollection(seq).name("source")

  val iteratingSource = source.iterate((input: ConnectedStreams[Int, String]) ⇒ {
    val head = input.map(i => (i + 1).toString, s => s)

    val feedback = head.filter(_.toInt %2 == 0).name("filter feedback")
    val output = head.filter(_.toInt %2  != 0).name("filter output")

    (feedback, output)
  }, 1000).name("iterate")


  iteratingSource.print().name("printer sink")

  println("execution plan:" + env.getExecutionPlan)

  env.execute()

}

object IterateDemo2 extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val someIntegers: DataStream[Long] = env.generateSequence(0, 1000)

  val iteratedStream = someIntegers.iterate(
    iteration => {
      val minusOne = iteration.map( v => v - 1)
      val stillGreaterThanZero = minusOne.filter (_ > 0)
      val lessThanZero = minusOne.filter(_ <= 0)
      (stillGreaterThanZero, lessThanZero)
    }
  )
  iteratedStream.print().name("printer sink")

  println("execution plan:" + env.getExecutionPlan)

  env.execute()

}
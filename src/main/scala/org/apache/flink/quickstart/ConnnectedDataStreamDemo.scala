package org.apache.flink.quickstart

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.quickstart.JoiningStreamDemo.env
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, StreamExecutionEnvironment}


import org.apache.flink.streaming.api.scala._

object ConnectedDataStreamDemo extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
  env.setParallelism(2)

  import SimpleSource._
  val source1 = new ThrottlingSource[Customer](new SimpleSource[Customer], 10)
  val source2 = new ThrottlingSource[Int](new SimpleSource[Int], 1)

  val firstInput: DataStream[Customer] = env.addSource(source1)
  val secondInput: DataStream[Int] = env.addSource(source2)

  val connectedStream = firstInput.connect(secondInput)

  val result: DataStream[String] = connectedStream.map(new CoMapFunction[Customer, Int, String]{

    var multiplier = 0
    override def map1(value: Customer) = {
      value.id + "  " + multiplier
    }
    override def map2(value: Int) =  {
      multiplier = Math.max(value, multiplier)

      ""
    }
  })

  result.filter(!_.isEmpty).print()
  env.execute()
}

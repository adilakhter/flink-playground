package org.apache.flink.quickstart

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{ConnectedCarEvent, GapSegment}
import com.dataartisans.flinktraining.exercises.datastream_java.utils.CompareByTimestampAscending
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.ProcessFunction._
import java.util.PriorityQueue

object ConnectedCar2 {

  def main(args: Array[String]) {

  // parse parameters
//  val params = ParameterTool.fromArgs(args)
//  val input = params.getRequired("input")

    val input: String = "/Users/adil/flink/carOutOfOrder.csv"

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // operate in Event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val cars = env.readTextFile(input)

    val events = cars
      .map(line ⇒ ConnectedCarEvent.fromString(line))
      .assignTimestampsAndWatermarks(new CarEventWatermarks())


    val eventsWithSortProcessFunction=
      events
      .keyBy("carId")
      .process(new SortProcessFunction())

    eventsWithSortProcessFunction.print()

    env.execute("Car Events with SortProcess Function")
  }

  class CarEventWatermarks extends AssignerWithPunctuatedWatermarks[ConnectedCarEvent] {
    val delay = 40*1000 // in 5 sec
    def checkAndGetNextWatermark(t: ConnectedCarEvent, l: Long): Watermark = new Watermark(l  - delay)
    def extractTimestamp(t: ConnectedCarEvent, l: Long): Long = t.timestamp
  }


  import collection.JavaConverters._
  class CreateGapSegment extends WindowFunction[ConnectedCarEvent, GapSegment, Tuple, TimeWindow] {
    def apply(key: Tuple, window: TimeWindow, input: Iterable[ConnectedCarEvent], out: Collector[GapSegment]): Unit =
      out.collect(new GapSegment(input.asJava))
  }

  class SortProcessFunction extends ProcessFunction[ConnectedCarEvent, ConnectedCarEvent] {

    lazy val state: ValueState[PriorityQueue[ConnectedCarEvent]]  =
      getRuntimeContext.getState(
        new ValueStateDescriptor[PriorityQueue[ConnectedCarEvent]](
          "sortState",
          classOf[PriorityQueue[ConnectedCarEvent]]))


    override def onTimer(timestamp: Long, ctx: ProcessFunction[ConnectedCarEvent, ConnectedCarEvent]#OnTimerContext, out: Collector[ConnectedCarEvent]): Unit = {

      println(ctx.timerService().currentProcessingTime())
      val queue = state.value()

      var head = queue.peek()
      while ((head != null) && (head.timestamp <= ctx.timerService().currentWatermark())) {
        queue.remove(head)

        out.collect(head)
        head = queue.peek()
      }
    }

    override def processElement(value: ConnectedCarEvent, ctx: ProcessFunction[ConnectedCarEvent, ConnectedCarEvent]#Context, out: Collector[ConnectedCarEvent]): Unit = {
      if(ctx.timestamp() >  ctx.timerService().currentWatermark()) {
        var currentState = state.value()

        if(currentState == null) {
          currentState = new PriorityQueue[ConnectedCarEvent](10, new CompareByTimestampAscending())
        }
        currentState.add(value)
        state.update(currentState)

        ctx.timerService.registerEventTimeTimer(value.timestamp)
      }
    }
  }
}
package org.apache.flink.quickstart


import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{ConnectedCarEvent, GapSegment}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

object ConnectedCar {

  def main(args: Array[String]) {

  // parse parameters
//  val params = ParameterTool.fromArgs(args)
//  val input = params.getRequired("input")

    val input: String = "/Users/adil/fastdata/carOutOfOrder.csv"

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val cars = env.readTextFile(input)

    val events = cars
      .map(line ⇒ ConnectedCarEvent.fromString(line))
      .assignTimestampsAndWatermarks(new CarEventWatermarks())


    val eventsKeyedBy: DataStream[GapSegment] =
      events
      .keyBy("carId")
      .window(EventTimeSessionWindows.withGap(Time.seconds(15)))
      .apply(new CreateGapSegment())



    eventsKeyedBy.writeAsText("/tmp/result", WriteMode.OVERWRITE).setParallelism(1)

    env.execute("Travel Time Prediction")
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

}
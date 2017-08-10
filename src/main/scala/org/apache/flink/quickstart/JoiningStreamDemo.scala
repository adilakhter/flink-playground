package org.apache.flink.quickstart


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, JoinedStreams, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object JoiningStreamDemo extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
  env.setParallelism(2)

  implicit val t1 = TypeInformation.of(classOf[Int])
  implicit val t2 = TypeInformation.of(classOf[(Customer, Int)])
  implicit val t3 = TypeInformation.of(classOf[Customer])


  import SimpleSource._

  val source1 = new ThrottlingSource[Customer](new SimpleSource[Customer], 10)
  val source2 = new ThrottlingSource[Int](new SimpleSource[Int], 1)


  val firstInput: DataStream[Customer] = env.addSource(source1)
  val secondInput: DataStream[Int] = env.addSource(source2)

  val firstKeyed = firstInput.keyBy(_.cardId)
  val secondKeyed = secondInput.keyBy(i ⇒ i)


  val result: JoinedStreams[Customer, Int] =
    firstKeyed.join(secondKeyed)

  val r =
    result
      .where((c: Customer) ⇒ c.cardId)
      .equalTo(identity)
      .window(TumblingEventTimeWindows.of(Time.seconds(3))).apply { (c, i) ⇒
      (c, i)
    }

  r.print().name("printer sink")
  env.execute()
}

package org.apache.flink.quickstart

import java.util.UUID

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}

import scala.reflect.ClassTag

package object kafka {

  val PreprocessingEventStatus = "PreProcessing"
  val FeatureScoringEventStatus = "FeatureScoring"
  val ProcessingDoneEventStatus = "ProcessingDone"

  case class ClickEvent(customer: String, clickedItem: String, eventtime: Long, status: String = PreprocessingEventStatus, id: String = UUID.randomUUID().toString)

  class ClickEventOps(targetTopic: String) extends KeyedSerializationSchema[ClickEvent] with KeyedDeserializationSchema[ClickEvent] {
    override def serializeKey(t: ClickEvent): Array[Byte] = {
      t.customer.getBytes("utf-8")
    }

    override def getTargetTopic(t: ClickEvent): String = {
      targetTopic
    }

    override def serializeValue(t: ClickEvent): Array[Byte] = {
      val testData = s"${t.customer}|${t.clickedItem}|${t.eventtime}|${t.status}|${t.id}"
      testData.getBytes("utf-8")
    }

    override def isEndOfStream(t: ClickEvent): Boolean = false

    override def deserialize(key: Array[Byte], message: Array[Byte], topicName: String, partition: Int, offset: Long): ClickEvent = {
      if (topicName == targetTopic) {
        val x = new String(message, "utf-8")
        val splits = x.split('|')
        ClickEvent(splits(0), splits(1), splits(2).toLong, splits(3), splits(4))
      } else {
        null
      }
    }

    override def getProducedType: TypeInformation[ClickEvent] = TypeExtractor.getForClass(implicitly[ClassTag[ClickEvent]].runtimeClass.asInstanceOf[Class[ClickEvent]])
  }


  def getAsSeq(ts:Long): Seq[ClickEvent] = Seq(
    //                                // eventTimer @ | Triggered After Event         | Collected Events
    ClickEvent("a", "A", ts + 0),     //     25       | ClickEvent("r", "A", ts + 30) |
    ClickEvent("b", "A", ts + 10),    //     35       | ClickEvent("q", "B", ts + 40) |
    ClickEvent("c", "A", ts + 20),    //     45       | ClickEvent("c", "B", ts + 50) |
    ClickEvent("r", "A", ts + 30),    //     55       | ClickEvent("c", "B", ts + 60) |
    ClickEvent("q", "B", ts + 40),    //     65       | ClickEvent("r", "B", ts + 70) |
    ClickEvent("c", "B", ts + 50),    //     75       | ClickEvent("q", "C", ts + 80) |
    ClickEvent("c", "B", ts + 60),    //     85       | ClickEvent("c", "C", ts + 90) |
    ClickEvent("r", "B", ts + 70),    //     95       | ClickEvent("c", "C", ts + 100)|
    ClickEvent("q", "C", ts + 80),    //              | ClickEvent("r", "C", ts + 110)|
    ClickEvent("c", "C", ts + 90),    //              | |
    ClickEvent("c", "C", ts + 100),   //              | |
    ClickEvent("c", "C", ts + 110)//,   //              | |
    //    ClickEvent("c", "C", ts + 120),   //              | |
    //    ClickEvent("c", "C", ts + 130),   //              | |
    //    ClickEvent("c", "C", ts + 140),   //              | |
    //    ClickEvent("c", "C", ts + 150),   //              | |
    //    ClickEvent("r", "C", ts + 160)    //              | |
  )
}

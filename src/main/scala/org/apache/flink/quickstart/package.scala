package org.apache.flink

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.util.Random

package object quickstart {

  case class Customer(id: String, cardId: Int)

  object SourceData {
    val Names: Array[String] = Array("tom", "jerry", "alice", "bob", "john", "grace")
    val CardId: Array[Int] = (1 to 10).toArray
  }

  class ThrottlingSource[A](iterator: Iterator[A], rate: Int) extends /*Parallel*/ SourceFunction[A] {
    private[this] val rnd = new Random(hashCode())
    private final val Millis = 1000
    var running = true

    override def run(ctx: SourceContext[A]): Unit = {
      while (running) {
        ctx.getCheckpointLock.synchronized {
          ctx.collect(iterator.next())
        }
        Thread.sleep(Millis / rate)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class CustomerSource extends Iterator[Customer] with Serializable {
    private[this] val rnd = new Random(hashCode())
    private[this] var prevUTC = System.currentTimeMillis() - 1000 * 3600 * 24 * 100 //100 days ago...

    def hasNext: Boolean = true

    def next: Customer = {
      Customer(SourceData.Names(rnd.nextInt(SourceData.Names.length)), SourceData.CardId(rnd.nextInt(SourceData.CardId.length)))
    }

    //TODO update prevUTC per customer
    def nextUTC: Long = {
      val newUtc = rnd.nextUTC(prevUTC)
      prevUTC = newUtc
      newUtc
    }
  }

  trait Buildable[T] extends Serializable {
    def build(rand: Random): T
  }

  object SimpleSource  {
    implicit object CustomerBuilder extends Buildable[Customer]{
      override def build(rnd: Random): Customer =
        Customer(SourceData.Names(rnd.nextInt(SourceData.Names.length)), SourceData.CardId(rnd.nextInt(SourceData.CardId.length)))
    }

    implicit object IntBuilder extends Buildable[Int] {
      override def build(rand: Random): Int = rand.nextInt(100)
    }
  }

  class SimpleSource[T:Buildable] extends Iterator[T] with Serializable {
    private[this] val rnd = new Random(hashCode())
    private[this] var prevUTC = System.currentTimeMillis() - 1000 * 3600 * 24 * 100 //100 days ago...

    def hasNext: Boolean = true

    def next: T = implicitly[Buildable[T]].build(rnd)

    //TODO update prevUTC per customer
    def nextUTC: Long = {
      val newUtc = rnd.nextUTC(prevUTC)
      prevUTC = newUtc
      newUtc
    }
  }


  type Location = (Double, Double) // (Latitude, Longitude)
  type UTCLocation = (Long, Location)

  implicit class RandomOps(e: Random) {
    val maxDelay: Int = 1000 * 3600 // 1 hour

    def nextLatitude(): Double = (e.nextDouble() * -180.0) + 90.0

    def nextLongitute(): Double = (e.nextDouble() * -360.0) + 180.0

    def nextLocation(): Location = (e.nextLatitude(), e.nextLongitute())

    def nextUTC(prevUtc: Long): Long = prevUtc + e.nextInt(maxDelay)
  }

}

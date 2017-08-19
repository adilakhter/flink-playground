package org.apache.flink.quickstart.kafka

import java.util.Random

import org.apache.flink.quickstart.Buildable
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class TickSource[A](tick: Random ⇒ A, interval: Long, initialDelay: Long = 0) extends SourceFunction[A] {
  private[this] val rnd = new Random(hashCode())
  private var running = true
  private var initiallyDelayed = false

  override def run(ctx: SourceContext[A]): Unit = {
    ensureSourceInitiallyDelayed()

    while (running) {
      ctx.getCheckpointLock.synchronized {
        ctx.collect(tick(rnd))
      }
      Thread.sleep(interval)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

  def ensureSourceInitiallyDelayed(): Unit = {
    if(!initiallyDelayed) {
      Thread.sleep(initialDelay)
      initiallyDelayed = true
    }
  }
}
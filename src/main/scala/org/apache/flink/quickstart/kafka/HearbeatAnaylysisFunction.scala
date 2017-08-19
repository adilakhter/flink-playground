package org.apache.flink.quickstart.kafka

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector

class HearbeatAnaylysisFunction extends CoFlatMapFunction[ClickEvent, Any, Any] {
  override def flatMap1(value: ClickEvent, out: Collector[Any]) = ???

  override def flatMap2(value: Any, out: Collector[Any]) = ???
}

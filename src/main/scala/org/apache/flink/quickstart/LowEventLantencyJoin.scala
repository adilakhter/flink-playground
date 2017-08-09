///*
// * Copyright 2017 data Artisans GmbH
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.quickstart
//
//import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{Customer, EnrichedTrade, Trade}
//import com.dataartisans.flinktraining.exercises.datastream_scala.lowlatencyjoin.{EventTimeJoinFunction, EventTimeJoinHelper}
//import com.dataartisans.flinktraining.exercises.datastream_scala.sources.FinSources
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.ProcessFunction
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.util.Collector
//
//object LowLatencyEventTimeJoin {
//  def main(args: Array[String]) {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    // Simulated trade stream
//    val tradeStream = FinSources.tradeSource(env)
//
//    // simulated customer stream
//    val customerStream = FinSources.customerSource(env)
//
//
//    tradeStream.print()
//    customerStream.print()
////
////    val joinedStream = tradeStream
////      .keyBy(_.customerId)
////      .connect(customerStream.keyBy(_.customerId))
////      .process(new EventTimeJoinFunction2())
////
////    joinedStream.print()
////
////    env.execute
//  }
//}
//
//class EventTimeJoinFunction2 extends EventTimeJoinHelper {
//
//  type Ctx = CoProcessFunction[Trade, Customer, EnrichedTrade]#Context
//  type OnTimerCtx = CoProcessFunction[Trade, Customer, EnrichedTrade]#OnTimerContext
//
//  override def processElement1(trade: Trade,
//                               context: Ctx,
//                               collector: Collector[EnrichedTrade]): Unit = {
//    println(s"Received: $trade")
//
//    val timerService = context.timerService()
//    val joinedData = join(trade)
//
//    collector.collect(joinedData)
//
//    if (context.timestamp() > timerService.currentWatermark()) {
//      enqueueEnrichedTrade(joinedData)
//      timerService.registerEventTimeTimer(trade.timestamp)
//    } else {
//      // Handle late data -- detect and join against what, latest?  Drop it?
//    }
//  }
//
//  override def processElement2(customer: Customer,
//                               context: Ctx,
//                               collector: Collector[EnrichedTrade]): Unit = {
//    println(s"Received $customer")
//    enqueueCustomer(customer)
//  }
//
//  override def onTimer(l: Long,
//                       context: OnTimerCtx,
//                       collector: Collector[EnrichedTrade]): Unit = {
//    // look for trades that can now be completed
//    val watermark: Long = context.timerService().currentWatermark()
//    while (timestampOfFirstTrade() <= watermark) {
//      // do the join and remove from the tradebuffer
//      dequeueAndPerhapsEmit(collector)
//    }
//
//    // Cleanup all the customer data that is eligible
//    cleanupEligibleCustomerData(watermark)
//  }
//
//  private def join(trade: Trade): EnrichedTrade = {
//    // get the customer info that was in effect at the time of this trade
//    // doing this rather than jumping straight to the latest known info makes
//    // this 100% deterministic.  If that's not a strict requirement we can simplify
//    // this by joining against the latest available data right now.
//    new EnrichedTrade(trade, getCustomerInfo(trade))
//  }
//
//  private def getCustomerInfo(trade: Trade): String = {
//    customerBufferState.value()
//      .filter(_.timestamp <= trade.timestamp)
//      .headOption
//      .map(_.customerInfo)
//      .getOrElse("No customer info available")
//  }
//
//  protected def dequeueAndPerhapsEmit(collector: Collector[EnrichedTrade]): Unit = {
//    val enrichedTrade = dequeueEnrichedTrade()
//
//    val joinedData = join(enrichedTrade.trade)
//    // Only emit again if we have better data
//    if (!joinedData.equals(enrichedTrade)) {
//      collector.collect(joinedData)
//    }
//  }
//}

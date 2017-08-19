package org.apache.flink.quickstart


import java.net.InetSocketAddress
import java.util.Random
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import com.codahale.metrics.graphite._

import scala.util.Try

object settings {

  val HeartbeatMetricsPrefixKey = "sdp.heartbeat"
  val HeartbeatMetricsRequestsSentKey = "requests"

  val HeartbeatMetricsPreprocessingDoneKey = "preprocessing.response"
  val HeartbeatMetricsPreprocessingResponseTimeKey = "preprocessing.response.time"

  val HeartbeatMetricsScoringResponseKey = "fexnscoring.response"
  val HeartbeatMetricsScoringResponseTimeKey = "fexnscoring.response.time"

  val HeartbeatMetricsPrefix = "sdp.heartbeat"
  val HeartbeatMetricsRequestsSent = "requests"
  val HeartbeatMetricsPreprocessingDone = "preprocessing.response"
  val HeartbeatMetricsScoringDone = "fexNScoring.response"
}

case class GraphiteServerDetails (hostname: String, port: Int)

case class GraphiteMetrics (serverDetails: GraphiteServerDetails) {
  import settings._
  val registry = new MetricRegistry()

  def newGraphite(): Graphite =
    new Graphite(new InetSocketAddress(serverDetails.hostname, serverDetails.port))

  def incrementCounter(metricName: String): Option[Counter] =
    if(enabled) {
      var counter = registry.getCounters.get(metricName)
      if (counter == null)
        counter = registry.counter(metricName)
      counter.inc(1)
      Some(counter)
    } else {
      None
    }

  def updateHistogram(metricName: String, value: Long): scala.Option[Histogram]  =
    if(enabled) {
      var histogram = registry.getHistograms.get(metricName)
      if(histogram == null) {
          histogram = registry.histogram(metricName)
      }
      histogram.update(value)
      Some(histogram)
    } else {
      None
    }

  val graphiteReporterOpt: Option[GraphiteReporter] =
    Try {
      val reporter =
        GraphiteReporter
          .forRegistry(registry)
          .prefixedWith(HeartbeatMetricsPrefix).convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .filter(MetricFilter.ALL)
          .build(newGraphite())

      reporter.start(1, TimeUnit.MINUTES)
      reporter

    }.toOption

  val enabled: Boolean = graphiteReporterOpt.isDefined

}

object TestGraphiteMetrices extends App {

  import settings._

  val graphiteServerDetails = GraphiteServerDetails("localhost", 2003)
  val graphiteMetrics = GraphiteMetrics(graphiteServerDetails)

  val random = new Random(hashCode())

  while(true) {

    graphiteMetrics.incrementCounter(HeartbeatMetricsScoringResponseKey)

    println("-")
    Thread.sleep(1000)

    graphiteMetrics.incrementCounter(HeartbeatMetricsScoringResponseKey)
    println("-")
    Thread.sleep(1000)

    graphiteMetrics.incrementCounter(HeartbeatMetricsScoringResponseKey)

    println("-")
    Thread.sleep(1000)

    graphiteMetrics.incrementCounter(HeartbeatMetricsScoringResponseKey)

    println("-")
    Thread.sleep(1000)

    graphiteMetrics.updateHistogram(HeartbeatMetricsScoringResponseTimeKey, random.nextInt(1000))

  }

}
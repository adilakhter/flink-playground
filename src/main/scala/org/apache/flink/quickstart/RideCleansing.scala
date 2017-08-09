package org.apache.flink.quickstart

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.{GeoUtils, TaxiRideSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010

object RideCleansing {

  def main(args: Array[String]) {


    // parse parameters
//    val params = ParameterTool.fromArgs(args)
//    val input = params.getRequired("input")

    val maxDelay = 60 // events are out of order by max 60 seconds
    val speed = 600   // events of 10 minutes are served in 1 second

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // get the taxi ride data stream
//    val rides = env.addSource(new TaxiRideSource(input, maxDelay, speed))

    val rides = env.addSource(
      new TaxiRideSource("nycTaxiRides.gz", maxDelay, speed))

    val filteredRides = rides
      // filter out rides that do not start and end in NYC`
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))

    filteredRides.addSink(new FlinkKafkaProducer010[TaxiRide](
      "localhost:9092",      // Kafka broker host:port
      "test",       // Topic to write to
      new TaxiRideSchema())  // Serializer (provided as util)
    )


    // print the filtered stream
    //filteredRides.print()

    // run the cleansing pipeline
    env.execute("Taxi Ride Cleansing")
  }

}

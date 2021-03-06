package org.apache.flink.quickstart

import java.util.concurrent.TimeUnit

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.{CheckpointedTaxiRideSource, TaxiRideSource}
import com.dataartisans.flinktraining.exercises.datastream_java.utils.{GeoUtils, TravelTimePredictionModel}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Scala reference implementation for the "Travel Time Prediction" exercise of the Flink training
  * (http://dataartisans.github.io/flink-training).
  *
  * The task of the exercise is to continuously train a regression model that predicts
  * the travel time of a taxi based on the information of taxi ride end events.
  * For taxi ride start events, the model should be queried to estimate its travel time.
  *
  * Parameters:
  * -input path-to-input-file
  *
  */
object TravelTimePrediction {

  def main(args: Array[String]) {

    // parse parameters
//    val params = ParameterTool.fromArgs(args)
//    val input = params.getRequired("input")

    val speed = 600 // events of 10 minutes are served in 1 second
    val localData: String = "/Users/adil/Downloads/nycTaxiRides.gz"


    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // operate in Event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // create a checkpoint every 5 seconds
    env.enableCheckpointing(1000)
    // try to restart 60 times with 10 seconds delay (10 Minutes)
    //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)))
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(6, Time.of(10, TimeUnit.SECONDS)))
    // get the taxi ride data stream
    val rides = env.addSource(new CheckpointedTaxiRideSource(localData, speed))

    val filteredRides = rides
      // filter out rides that do not start and end in NYC
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      // map taxi ride events to the grid cell of the destination
      .map(r => (GeoUtils.mapToGridCell(r.endLon, r.endLat), r))
      // organize stream by destination
      .keyBy(_._1)
      // predict and refine model per destination
      .flatMap(new PredictionModel())

    // print the predictions
    filteredRides.print()

    // run the prediction pipeline
    env.execute("Travel Time Prediction")
  }

  /**
    * Predicts the travel time for taxi ride start events based on distance and direction.
    * Incrementally trains a regression model using taxi ride end events.
    */
  class PredictionModel extends RichFlatMapFunction[(Int, TaxiRide), (Long, Int)] {

    var modelState: ValueState[TravelTimePredictionModel] = _

    override def flatMap(in: (Int, TaxiRide), out: Collector[(Long, Int)]): Unit = {

      // fetch operator state
      val model: TravelTimePredictionModel = modelState.value
      val ride: TaxiRide = in._2

      // compute distance and direction
      val distance = GeoUtils.getEuclideanDistance(ride.startLon, ride.startLat, ride.endLon, ride.endLat)
      val direction = GeoUtils.getDirectionAngle(ride.endLon, ride.endLat, ride.startLon, ride.startLat)

      if (ride.isStart) {
        // we have a start event: Predict travel time
        val predictedTime: Int = model.predictTravelTime(direction, distance)
        // emit prediction
        out.collect((ride.rideId, predictedTime))
      }
      else {
        // we have an end event: Update model
        // compute travel time in minutes
        val travelTime = (ride.endTime.getMillis - ride.startTime.getMillis) / 60000.0
        // refine model
        model.refineModel(direction, distance, travelTime)
        // update operator state
        modelState.update(model)
      }
    }

    override def open(config: Configuration): Unit = {
      // obtain key-value state for prediction model
      val descriptor = new ValueStateDescriptor[TravelTimePredictionModel](
        // state name
        "regressionModel",
        // type info for state object
        TypeInformation.of(new TypeHint[TravelTimePredictionModel]() {}),
        // state default value
        new TravelTimePredictionModel)

      modelState = getRuntimeContext.getState(descriptor)
    }
  }

}
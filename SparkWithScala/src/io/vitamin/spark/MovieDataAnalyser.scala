package io.vitamin.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Codec

/**
  * Created by vitamin on 07/04/2017.
  */
object MovieDataAnalyser {


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "MovieDataAnalyser")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/u.data")

    // (The file format is userID, movieID, rating, timestamp)
    val movieRDD = lines.map(x => x.split("\t"))

    // Print some sample data
    //movieRDD.take(5).map(arr => arr.mkString(", ")).foreach(println)

    //Most Viewed Movie
    val movieByNumberOfWatch = movieRDD.map(arr => (arr(1), 1)).reduceByKey((x, y) => x + y)
    val sortedMovieByNumberOfWatch =  movieByNumberOfWatch.sortBy(p => p._2, ascending = false)
    sortedMovieByNumberOfWatch.take(5).foreach(println)

    println((1 to 20).map( i => "-").mkString)

    //Highest Average rated movies
    val totalRatesByMovie = movieRDD.map(arr => (arr(1), (arr(2).toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val sortedAvarageRateByMovie = totalRatesByMovie.map(d => (d._1, d._2._1 / d._2._2))
      .sortBy( p => p._2, ascending = false)

    sortedAvarageRateByMovie.take(5).foreach(println)
  }
}

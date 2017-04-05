package io.vitamin.spark

import java.util.concurrent.ThreadLocalRandom

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by vitamin on 05/04/2017.
  */
object GroupByAgeRDD {
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
    /* Setting up the random number generator*/
    val localRandom = ThreadLocalRandom.current()

    /* Data : Age, Number of Friends */
    val data = (1 to 1000).map( _ => (localRandom.nextInt(18, 120), localRandom.nextInt(10, 750)))
    val ageWithFriendsRDD = sc.parallelize(data)

    // (Age, (#Friends, 1))
    val withFriends = ageWithFriendsRDD.mapValues(x => (x, 1))

    //(Age, (#TotalFriends, #OfPeopleWithThatAge)
    val total = withFriends.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    total.take(5).foreach(println)

    //(Age, Mean)
    val mean = total.mapValues(x => x._1 / x._2)
    mean.take(5).foreach(println)
  }
}

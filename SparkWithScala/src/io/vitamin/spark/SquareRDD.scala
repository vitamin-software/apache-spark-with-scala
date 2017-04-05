package io.vitamin.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by vitamin on 05/04/2017.
  */
object SquareRDD {
  def main(args: Array[String]): Unit = {
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)

      val sc = new SparkContext("local[*]", "Sample-1")
      val rdd = sc.parallelize(List(1,2,3,4))
      val sq = rdd.map(x => x * x)
      println(sq.collect().mkString(", "))

      /* Actions:
         collect => gets all values within RDD
         count   => row count of RDD
         countByValue => count by unique value
         take(n) => first n rows
         top => similar to take
         reduce => functional reduce execution

         Until you call an action - nothing happens
         Lazy Evaluation
       */
  }
}

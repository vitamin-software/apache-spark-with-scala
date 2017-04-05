package io.vitamin.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by vitamin on 05/04/2017.
  */
object MinTempByLocation {

  def parseLine(line:String) = {
      val tokens = line.split(",")
      val location = tokens(0)
      val date = tokens(1)
      val tempType = tokens(2)
      val temp = tokens(3).toFloat * 0.1f

      (location, (date, tempType, temp))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MinTempByLocation")
    val dataRDD = sc.textFile("../data/1800.csv").map(parseLine)

    val criteria = "TMAX" // "TMIN"
    val fMin = (x:(String, String, Float), y:(String, String, Float)) =>  if (x._3 <= y._3) x else y
    val fMax = (x:(String, String, Float), y:(String, String, Float)) =>  if (x._3 >= y._3) x else y

    val tempMin = dataRDD.filter( p => p._2._2.trim == criteria)
    tempMin.take(3).foreach(println)
    println("------------")

    val minTempByLocation = tempMin.reduceByKey(fMax)
    val result = minTempByLocation.collect().sorted

    //TODO there must be better way to format
    result.map(d => (d._1, d._2._3, d._2._1))
      .map(d => "%s had temperature %.2f Celsius on date %s".format (d._1, d._2, d._3)).foreach(println)
  }

}

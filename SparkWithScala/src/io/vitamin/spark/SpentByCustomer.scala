package io.vitamin.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by vitamin on 07/04/2017.
  */
object SpentByCustomer {
  def parseLine(line:String) = {
     val data = line.split(",")
     (data(0), data(2).toFloat)
  }
  def main(args: Array[String]) ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "SpentByCustomer")

    /*Load customer data*/
    val dataRDD = sc.textFile("../data/customer-orders.csv").map(parseLine)

    /*Calculate spending for per customer*/
    val byCustomer = dataRDD.reduceByKey( (x, y) => x + y)

    /*order by spending per customer*/
    val byCustomerSorted = byCustomer.sortBy( c => c._2, ascending = false)

    /*Get top 50 customer*/
    val top50 = byCustomerSorted.take(50)
    top50.map(c => "customer:%s spending:%.4f".format(c._1, c._2)).foreach(println)
  }
}

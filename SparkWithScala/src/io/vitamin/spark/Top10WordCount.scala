package io.vitamin.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Codec

/**
  * Created by vitamin on 06/04/2017.
  */
object Top10WordCount {

   def main(args: Array[String]) ={
     Logger.getLogger("org").setLevel(Level.ERROR)

     implicit val codec = Codec("UTF-8")
     codec.onMalformedInput(CodingErrorAction.REPLACE)
     codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

     val sc = new SparkContext("local[*]", "Top10WordCount")
     val dataRDD = sc.textFile("../data/book.txt")

     //val lines = dataRDD.map(line => line.replaceAll("""[\p{Punct}&&[^.]]""", ""))
     /*Pulling the words*/
     val words = dataRDD.flatMap(line => line.split("\\W+")) // ignores punctuation

     /*Counting occurrence of each word */
     val wordToCount = words.map(_.toLowerCase).map(w => (w, 1)).reduceByKey((x, y) => x + y)

     /*sorting by count*/
     val ordered = wordToCount.sortBy(e => e._2, ascending = false)

     /*taking top 5 words*/
     ordered.take(10).foreach(println)
   }
}

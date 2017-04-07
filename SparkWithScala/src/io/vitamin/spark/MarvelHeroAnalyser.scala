package io.vitamin.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

/**
  * Created by vitamin on 07/04/2017.
  */
object MarvelHeroAnalyser {

  /** Load up a id to names. */
  def loadNames(fileName:String, splitter:String = " ") : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)


    val idNamePair = Source.fromFile(fileName).getLines()
      .map( l => l.split(splitter))
      .filter(l => l.length > 1)
      .map(fields => (fields(0).trim.toInt, fields(1)))
      .toMap

    return idNamePair
  }

   def main(args:Array[String]) = {
     Logger.getLogger("org").setLevel(Level.ERROR)

     val sc = new SparkContext("local[*]", "MarvelHeroAnalyser")

     val heroIdToName = loadNames("../data/Marvel-names.txt", "\"")
     // Create a broadcast variable of our ID -> name map
     var nameDict = sc.broadcast(heroIdToName)

     val lines = sc.textFile("../data/Marvel-graph.txt")
     val heroes = lines.flatMap(line => line.split("\\s+")) // white space splitting
     val heroesCounter = heroes.map( hero => (hero.toInt, 1)).reduceByKey((x, y) => x + y)
     val popularHeroes = heroesCounter.sortBy( heroAndCount => heroAndCount._2, ascending = false )
       .map(x  => (nameDict.value(x._1), x._2) )
     popularHeroes.take(20).foreach(println)
   }
}

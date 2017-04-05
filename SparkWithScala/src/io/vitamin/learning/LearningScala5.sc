/**
  * Created by vitamin on 05/04/2017.
  */

object LearningScala5{
  def parseLine(posKey:Int)(line: String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val key = fields(posKey)
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (key, numFriends)
  }

  val parseForPos = parseLine(2) _
  println(parseForPos("1,2,3,4"))

  val p = ("Loc", 3.14f,"1234")
  println("%s had temperature %.2f Celsius on date %s".format (p._1, p._2, p._3))
  val d = List(p)
  d.map( p => "%s had temperature %.2f Celsius on date %s".format (p._1, p._2, p._3)).foreach(println)
}
name := "MovieSimilarities1M"

version := "1.0"

organization := "io.vitamin"

scalaVersion := "2.11.8"

mainClass in Compile := Some("io.vitamin.spark.MovieSimilarities1M")

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
)

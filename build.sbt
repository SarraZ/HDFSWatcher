name := "HDFSWatcher"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.0"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.8.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11"  % "2.4.0"

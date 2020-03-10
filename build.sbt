name := "spark-structured-streaming-fun"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.3.1",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.1",
  "org.twitter4j" % "twitter4j-stream" % "4.0.2",
  "org.twitter4j" % "twitter4j-core" % "4.0.2",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1")
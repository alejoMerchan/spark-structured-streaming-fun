package com.fun.streaming

import com.fun.streaming.model.TwitterEvent
import org.apache.spark.sql.SparkSession
import twitter4j.TwitterObjectFactory

object KafkaStreaming extends App {

  val spark = SparkSession.builder().appName("batch processing").master("local").getOrCreate()

  import spark.implicits._


  val rawData = spark.readStream.format("kafka").
    option("kafka.bootstrap.servers", "localhost:19092").
    option("subscribe", "my_topic").
    option("failOnDataLoss", "false").
    option("group.id", "t1").load()

  rawData.printSchema()

  val query = rawData.selectExpr("CAST(value AS STRING)").select("value").as[String].map {
    line =>
      val status = TwitterObjectFactory.createStatus(line)
      TwitterEvent(status.getUser.getName, System.currentTimeMillis())
  }


  query.writeStream.format("console").option("truncate", false).start().awaitTermination()


}

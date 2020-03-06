package com.fun.streaming

import com.fun.batch.WebLog
import com.fun.server.SocketHandler
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._


object StreamingMain extends App {


  val serverPort = 9999

  val spark = SparkSession.builder().appName("batch processing").master("local").getOrCreate()

  import spark.implicits._

  val rawLogs = spark.read.json("C:/data/nasa_dataset_july_1995")

  val preparedLogs = rawLogs.withColumn("http_reply", rawLogs.col("http_reply").cast(IntegerType))

  val weblogs = preparedLogs.as[WebLog]


  val server = new SocketHandler(spark, serverPort, weblogs)

  server.start()


  val stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

  val webLogSchema = Encoders.product[WebLog].schema

  val jsonStream = stream.select(from_json($"value",webLogSchema) as "record")

  val webLogStream: Dataset[WebLog] = jsonStream.select("record.*").as[WebLog]

  println("--- is streaming: " + webLogStream.isStreaming)


}

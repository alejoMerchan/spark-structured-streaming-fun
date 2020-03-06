package com.fun.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object BatchMain extends App {


  val spark = SparkSession.builder().appName("batch processing").master("local").getOrCreate()

  import spark.implicits._

  val rawLogs = spark.read.json("C:/data/nasa_dataset_july_1995")

  val preparedLogs = rawLogs.withColumn("http_reply", rawLogs.col("http_reply").cast(IntegerType))

  val weblogs = preparedLogs.as[WebLog]

  /**
   * A common question would be, what was the most popular URL per day?
   */
  val topDailyURLs = weblogs.withColumn("dayOFMonth", dayofmonth($"timestamp"))
    .select($"request",$"dayOfMonth").groupBy($"dayOfMonth",$"request")
    .agg(count($"request").alias("count")).orderBy(desc("count"))

  topDailyURLs.show()


}

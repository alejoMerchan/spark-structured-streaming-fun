package com.fun.streaming

import com.fun.batch.WebLog
import com.fun.server.SocketHandler
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._


object StreamingMain extends App {

  System.setProperty("hadoop.home.dir","C:/hadoop/")

  val serverPort = 9999

  val spark = SparkSession.builder().appName("batch processing").master("local")
    //.config("spark.driver.bindAddress","0.0.0.0")
    //.config("spark.driver.host","172.18.1.194")
    //.config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

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



  // A regex expression to extract the accessed URL from weblog.request
  val urlExtractor = """^GET (.+) HTTP/\d.\d""".r
  val allowedExtensions = Set(".html",".htm", "")

  val contentPageLogs: String => Boolean = url => {
    val ext = url.takeRight(5).dropWhile(c => c != '.')
    allowedExtensions.contains(ext)
  }

  val urlWebLogStream = webLogStream.flatMap{ weblog =>
    weblog.request match {
      case urlExtractor(url) if (contentPageLogs(url)) => Some(weblog.copy(request = url))
      case _ => None
    }
  }


  val rankingURLStream = urlWebLogStream.groupBy($"request", window($"timestamp", "5 minutes", "1 minute")).count()

  val query = rankingURLStream.writeStream
    .queryName("urlranks")
    .outputMode("complete")
    .format("memory")
    .start()

  spark.sql("show tables").show()



}

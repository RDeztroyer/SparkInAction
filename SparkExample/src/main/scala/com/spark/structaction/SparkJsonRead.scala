package com.spark.structaction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkJsonRead extends App {

  val spark =
    SparkSession.builder()
      .appName("Spark Read Json")
      .master("local[4]")
      .master("local[4]")
      .getOrCreate()

  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getRootLogger.setLevel(Level.ERROR)

  val fileStream = getClass.getResourceAsStream("/xx.json")
  val json = try scala.io.Source.fromInputStream(fileStream).getLines.mkString finally fileStream.close()
  val df = spark.read.json(Seq(json).toDS())
  df.select("pi.name").show(false)
}
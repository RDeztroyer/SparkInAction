package com.spark.structaction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object SparkRemoveFieldsOnRead extends App {

  val spark =
    SparkSession.builder()
      .appName("SQL-Basic")
      .master("local[4]")
      .master("local[4]")
      .getOrCreate()

  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getRootLogger.setLevel(Level.ERROR)

  val excludeList = Seq("samList.point","samKey","samObject.samPk")
  def recGetNames(struct: StructType, name: String, acc: Seq[StructField]): Seq[StructField] = {
    struct.fields.flatMap{
      case StructField(stName, dataType:StructType, nullable, _) => {
        val res = recGetNames(dataType, name +stName+".", Seq[StructField]())
        if (res.nonEmpty) acc :+ StructField(stName, new StructType(res.toArray),nullable) else acc
      }
      case StructField(stName, dataType:ArrayType, nullable, _) if dataType.elementType.isInstanceOf[StructType] => {
        val res = recGetNames(dataType.elementType.asInstanceOf[StructType], name +stName+".", Seq[StructField]())
        if (res.nonEmpty ) acc :+ StructField(stName, new ArrayType(new StructType(res.toArray),dataType.containsNull),nullable) else acc
      }
      case StructField(stName, dataType:ArrayType, nullable, _) if !dataType.elementType.isInstanceOf[StructType] => {
        if (excludeList contains  name +stName) acc else acc :+ StructField(stName,dataType,nullable)
      }
      case StructField(stName, dataType, nullable, _) => {
        if (excludeList contains  name +stName) acc else acc :+ StructField(stName,dataType,nullable)
      }
      case _ => acc
    }
  }

  val json = """{ "samKey": "1541102400000-VV3456-DXB", "samValue": "VV", "cap": "F012J042Y310", "samList": [ { "samPk": "1541102400000-VV3456-DXB", "samValue": "VV", "point": "DXB", "samKey": "1541102400000-VV3456-DXB" }, { "samPk": "1541102400000-VV3456-DXB1", "samValue": "VV", "point": "DXB", "samKey": "1541102400000-VV3456-DXB" } ], "samObject": { "samPk": "1541102400000-VV3456-DXB", "prefix": "VV", "point": "DXB", "samKey": "1541102400000-VV3456-DXB" } }"""
  val df = spark.read.json(Seq(json).toDS())
  val newschema = new StructType(recGetNames(df.schema,"",Seq()).toArray)
  val newdfwithremovedelements = spark.read.schema(newschema).json(Seq(json).toDS())
  newdfwithremovedelements.show(false)
  //df.show(false)
}
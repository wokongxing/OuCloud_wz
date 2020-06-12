package hw.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def createSparkSession(appName:String) ={

    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.broadcastTimeout", 20 * 60)
      .config("spark.sql.crossJoin.enabled", true)
      .config("odps.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.catalogImplementation", "odps")
      .getOrCreate()

    spark
  }

}

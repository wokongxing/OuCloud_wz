package main.scala.com.hw.test

import org.apache.spark.sql.SparkSession

object DeltaTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()


//    val data = spark.range(0, 5)
//    data.write.format("delta").save("/outdata/delta-table")

    val data = spark.range(5, 10)
    //data.write.format("delta").mode("overwrite").save("/outdata/delta-table")

    val df = spark.read.format("delta").load("delta-table")
    //指定 versionAsOf 参数用来访问数据表的一个 snapshot
//    val df = spark.read.format("delta").option("versionAsOf", 4).load("/outdata/delta-table")

    df.show()


    spark.stop()

  }

}

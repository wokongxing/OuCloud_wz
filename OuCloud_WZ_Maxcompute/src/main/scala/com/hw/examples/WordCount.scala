package hw.examples

import org.apache.spark.sql.SparkSession


object WordCount {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("WordCount")
//      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    try {
      sc.parallelize(1 to 100, 10).map(word => (word, 1)).reduceByKey(_ + _, 10).take(100).foreach(println)
    } finally {
      sc.stop()
    }
  }
}

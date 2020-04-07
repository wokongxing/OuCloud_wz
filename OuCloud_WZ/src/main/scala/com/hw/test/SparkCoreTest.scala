package main.scala.com.hw.test

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

object SparkCoreTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(sparkConf)

//    val rdd = sc.textFile("inputdata/*")
//    val collect = rdd.collect()

    val path ="inputdata/prewarn.txt"
    sc.addFile(path)
    val p =SparkFiles.get(path)
    val p2 =SparkFiles.getRootDirectory()
    println(p)
    println(p2)
    val rdd = sc.textFile(p).foreach(x=>println(x))



    sc.stop()

  }

}

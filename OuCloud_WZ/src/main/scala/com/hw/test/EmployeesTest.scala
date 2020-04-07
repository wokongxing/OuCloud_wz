package com.hw.test

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author linzhy
 */
object EmployeesTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()
    val url = config.getString("pg.datasync.url")
    val user = config.getString("pg.datasync.user")
    val password = config.getString("pg.datasync.password")


    val area_number_v1 = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "area_number_v1")
      .option("user", user)
      .option("password", password)
      .load()

    val area_number = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "area_number")
      .option("user", user)
      .option("password", password)
      .load()

    area_number_v1.createOrReplaceTempView("area_number_v1")
    area_number.createOrReplaceTempView("area_number")
    val dates = Array("2019-12-01","2020-01-01","2020-02-01")
    val datetime ="-2"

//    var sql=
//      s"""
//        |SELECT
//        |   date_format('${datetime}','yyyy-MM-dd HH:mm:ss') starttime,
//        |   year('${datetime}'),
//        |   month('${datetime}'),
//        |   add_months('${datetime}',1)
//        |FROM  area_number
//        | where code=152121
//        |""".stripMargin
    var real_sql = new StringBuffer()
      var sql=
        s"""
           |SELECT
           |   trunc(date_add(now(), -1),'MM')  cur_date,
           |   year(date_add(now(), -1))  cur_date_year,
           |   month(date_add(now(), -1)) cur_date_month,
           |   trunc(date_add(now(), -1),'MM')  cur_date,
           |   date_add(now(), -1),
           |    year(add_months(now(),${datetime})),
           |    month(add_months(now(),${datetime})),
           |    date_trunc('MM',add_months(now(),${datetime}))
           |FROM  area_number
           | where code=152121
           |""".stripMargin
   //保存数据
//    spark.sql(sql).write.format("jdbc")
//      .mode(SaveMode.Append)
//      .option("dbtable","area_number1")
//      .option("url",url)
//      .option("user",user)
//      .option("password",password)
//      .save()
    //    val tableSchema = spark.sql(sql).schema
    //    val columns =tableSchema.fields.map(x => x.name).mkString(",")
    //      println(columns)
    //    val placeholders = tableSchema.fields.map(_ => "?").mkString(",")
    //
    //    println(s"INSERT INTO  ($columns) VALUES ($placeholders)")

    val frame = spark.sql(sql)
    frame.show(10)
    frame.printSchema()

    spark.stop();
  }
}

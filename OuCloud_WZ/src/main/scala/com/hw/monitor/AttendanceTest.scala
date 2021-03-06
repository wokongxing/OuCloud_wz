package hw.monitor

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object AttendanceTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[3]")
      .config("spark.debug.maxToStringFields", "100")
      .config("spark.sendEmail.OnTaskFail.enabled", "true")
      .config("spark.extraListeners", "hw.monitor.TestMonitoring")
      .getOrCreate()

    var config = ConfigFactory.load()

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    val kq_attendances = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "kq_attendances")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    kq_attendances.createOrReplaceTempView("kq_attendances")

    val startdatetime = "0"
    val sql3 =
      s"""
        |
        |SELECT
        |   pid,
        |   sn,
        |   direct,
        |   idcardno,
        |   full_name,
        |   ifnull(mobile,"无") mobile,
        |   tid,
        |   record_time,
        |   ifnull(insert_time,record_time) insert_time,
        |   equp_name,
        |   photo_path,
        |   ifnull(app_key,'无') app_key,
        |   date_trunc('MM',add_months(now(),${startdatetime}))
        |FROM
        |   kq_attendances
        |where idcardno is not null
        |and insert_time is null
        |and record_time >=  date_trunc('MM',add_months(now(),${startdatetime}))
        |""".stripMargin
    val dataFrame = spark.sql(sql3)
    dataFrame.show(10)
    dataFrame.printSchema()
//
//    val tableSchema = dataFrame.schema
//    val columns =tableSchema.fields.map(x => x.name).mkString(",")
//
//    val update = tableSchema.fields.map(x =>
//      x.name.toString + "=?"
//    ).mkString(",")
//
//    println(update)

    spark.stop()

  }

}

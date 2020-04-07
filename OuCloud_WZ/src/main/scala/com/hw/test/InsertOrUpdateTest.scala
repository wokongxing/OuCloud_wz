package main.scala.com.hw.test

import com.typesafe.config.ConfigFactory
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.postgresql.core.BaseConnection

/**
 *测试用例
 *    批量保存数据,存在则更新 不存在 则插入
 *    INSERT INTO test_001 VALUES( ?, ?, ? )
 *    ON conflict ( ID ) DO
 *    UPDATE SET id=?,NAME = ?,age = ?;
 * @author linzhy
 */
object InsertOrUpdateTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    var config = ConfigFactory.load()
    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")
    val ads_url = config.getString("pg.oucloud_ads.url")
    val ads_user = config.getString("pg.oucloud_ads.user")
    val ads_password = config.getString("pg.oucloud_ads.password")

    //读取comapanysday表 信息
    val ads_companys_total = spark.read.format("jdbc")
      .option("url", ads_url)
      .option("dbtable","ads_companys_total")
      .option("user", ads_user)
      .option("password", ads_password)
      .load()

    val kq_attendances = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "kq_attendances")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    kq_attendances.createOrReplaceTempView("kq_attendances")
    ads_companys_total.createOrReplaceTempView("ads_companys_total")

    val sql=
      """
        |select
        | *
        |from
        | ads_companys_total AT
        |""".stripMargin

    val dataFrame = spark.sql(sql)
    dataFrame.printSchema()
//    dataFrame.show(100)
    //批量保存数据,存在则更新 不存在 则插入
    val conn = PgSqlUtil.connectionPool("OuCloud_ADS")
    PgSqlUtil.insertOrUpdateToPgsql(conn,dataFrame,spark.sparkContext,"ads_companys_total","pkid")

    spark.stop();
  }
}

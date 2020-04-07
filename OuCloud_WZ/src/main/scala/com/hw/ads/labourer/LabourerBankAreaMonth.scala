package main.scala.com.hw.ads.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/** 需求:
 *    统计每个月份,每一个区域,每一个银行所发薪资信息:
 *      发薪金额,发薪批次数量,发放人数
 *
 *  数据来源:
 *      CDM层: labourer_salary_month 发薪聚合信息表
 *
 *  获取字段:
 *      区域:--county_code,
 *      年份: --year;
 *      月份: --month,
 *      银行名称:--bank_name;
 *      发薪总金额:--real_amt_total,
 *      发薪人数总数:--labourer_total,
 *      发薪的项目数:--project_total;
 *
 * @author linzhy
 */
object LabourerBankAreaMonth{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                  .appName(this.getClass.getSimpleName)
                  .master("local[2]")
                  .getOrCreate()

    val config = ConfigFactory.load()

    queryDataCreateView(spark,config)

    val sql =
      """
        | SELECT
        |	  LSM.YEAR,
        |	  LSM.MONTH,
        | 	LSM.city_name,
        | 	LSM.county_name,
        |	  LSM.bank_name,
        |	  SUM ( LSM.ss_real_amt ) real_amt,
        | 	SUM ( LSM.person_sum ) real_number,
        |	  COUNT( DISTINCT LSM.pid ) project_total
        | FROM
        |	  labourer_salary_month LSM
        | GROUP BY
        |	  LSM.YEAR,
        |	  LSM.MONTH,
        |	  LSM.county_name,
        |	  LSM.city_name,
        |   LSM.bank_name
        |""".stripMargin


    val dataFrame = spark.sql(sql)
    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"labourer_bank_area_month")

//    dataFrame.show(1000)

    spark.stop()

  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")

    //cdm层 发薪信息表
    val labourer_salary_month = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "labourer_salary_month")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    labourer_salary_month.createOrReplaceTempView("labourer_salary_month")



  }
}

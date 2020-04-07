package main.scala.com.hw.ads.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

/** 需求:
 *    统计每个月份,每一个区域所发薪资信息:
 *      发薪金额,发薪批次数量,发放人数,企业发薪金额占比(分施工,劳务)
 *
 *  数据来源:
 *      CDM层: labourer_salary_month 发薪聚合信息表
 *  条件:
 *    发薪状态:
 *      status 1-成功 6--待确认 7 --发放中, 9--异常
 *  获取字段:
 *      区域:--county_code,
 *      年份: --year;
 *      月份: --month,
 *      发薪总金额:--real_amt_total,
 *      批次号总数:--batch_total,
 *      发薪人数总数:--labourer_total,
 *      企业类型：1：建设集团，2：施工单位，3：劳务企业，4：设计单位
 *      施工单位企业发薪金额总数:--construction_amt_total,
 *      劳务单位发薪金额总数:--labour_amt_total
 *
 * @author linzhy
 */
object LabourerSalaryAreaMonth{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                  .appName(this.getClass.getSimpleName)
                  .master("local[2]")
                  .getOrCreate()

    val config = ConfigFactory.load()

    queryDataCreateView(spark,config)

    val sql =
      """
        |   SELECT
        |     province_name,
        |     city_name,
        |     county_name,
        |     year,
        |     month,
        |     sum ( ss_real_amt ) real_amt_total,
        |     count ( batch_no ) batch_total,
        |     sum ( person_sum ) labourer_total,
        |     sum ( case when company_type=1 then ss_real_amt else 0 end ) build_amt_total,
        |     sum ( case when company_type=2 then ss_real_amt else 0 end ) construction_amt_total,
        |     sum ( case when company_type=3 then ss_real_amt else 0 end ) labour_amt_total,
        |     sum ( case when company_type=4 then ss_real_amt else 0 end ) epc_amt_total
        |   FROM
        |     labourer_salary_month
        |   WHERE status=1 or status=6
        |   GROUP BY year,month,
        |     province_name,
        |     city_name,
        |     county_name
        |
        |""".stripMargin


    val dataFrame = spark.sql(sql)
//    dataFrame.show(1000)
    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"labourer_salary_area_month")


    spark.stop()

  }

  //读取数据库数据
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

package main.scala.com.hw.ads.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{SaveMode, SparkSession}

/** 需求:
 *    统计每一个区域所发薪资信息:
 *      发薪金额,发薪批次数量,发放人数,企业发薪金额占比(分施工,劳务)
 *      上个月发薪总额,发薪批次数量,发薪总人数,企业发薪金额占比(分施工,劳务)
 *  数据来源:
 *      ADS层: labourer_salary_area_month 每月 每区域统计数据
 *
 *  获取字段:
 *      区域:--county_code,
 *      发薪总金额:--real_amt_total,
 *      批次号总数:--batch_total,
 *      发薪人数总数:--labourer_total,
 *      施工单位企业发薪金额总数:--construction_amt_total,
 *      劳务单位发薪金额总数:--labour_amt_total
 *    上个月:
 *       发薪总金额:--lastmonth_real_amt_total,
 *       批次号总数:--lastmonth_batch_total,
 *       发薪人数总数:--lastmonth_labourer_total,
 *       施工单位企业发薪金额总数:--lastmonth_construction_amt_total,
 *       劳务单位发薪金额总数:--lastmonth_labour_amt_total
 *
 * @author linzhy
 */
object LabourerSalaryArea{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                  .appName(this.getClass.getSimpleName)
                  .master("local[2]")
                  .getOrCreate()

    var config = ConfigFactory.load()
    val ads_url = config.getString("pg.oucloud_ads.url")
    val ads_user = config.getString("pg.oucloud_ads.user")
    val ads_password = config.getString("pg.oucloud_ads.password")

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    //cdm层 发薪信息表
    val labourer_salary_area_month = spark.read.format("jdbc")
      .option("url", ads_url)
      .option("dbtable", "labourer_salary_area_month")
      .option("user", ads_user)
      .option("password", ads_password)
      .load()

    //字典区域表
    val aj_dict_area = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "aj_dict_area")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    //温州区域
    aj_dict_area.where("parent_code='330300' and is_deleted=1").createOrReplaceTempView("aj_dict_area")
    labourer_salary_area_month.createOrReplaceTempView("labourer_salary_area_month")

    val sql =
      """
        |SELECT
        | AREA.name county_name,
        | LS.real_amt_total,
        | LS.batch_total,
        | LS.labourer_total,
        | LS.construction_amt_total,
        | CONCAT ( ROUND( LS.construction_amt_total / LS.real_amt_total * 100, 2 ), '%' ) construction_proportion,
        | LS.labour_amt_total,
        | CONCAT ( ROUND( LS.labour_amt_total / LS.real_amt_total * 100, 2 ), '%' ) labour_proportion,
        |  LS.build_amt_total,
        | CONCAT ( ROUND( LS.build_amt_total / LS.real_amt_total * 100, 2 ), '%' ) build_proportion,
        |  LS.epc_amt_total,
        | CONCAT ( ROUND( LS.epc_amt_total / LS.real_amt_total * 100, 2 ), '%' ) epc_proportion,
        | LSYM.year,
        | LSYM.month,
        | LSYM.real_amt_total as lastmonth_real_amt_total,
        | LSYM.batch_total as lastmonth_batch_total,
        | LSYM.labourer_total as lastmonth_labourer_total,
        | LSYM.construction_amt_total as lastmonth_construction_amt_total,
        | CONCAT ( ROUND( LSYM.construction_amt_total / LSYM.real_amt_total * 100, 2 ), '%' ) lastmonth_construction_proportion,
        | LSYM.labour_amt_total as lastmonth_labour_amt_total,
        | CONCAT ( ROUND( LSYM.labour_amt_total / LSYM.real_amt_total * 100, 2 ), '%' ) lastmonth_labour_proportion,
        | LSYM.build_amt_total lastmonth_build_amt_total,
        | CONCAT ( ROUND( LSYM.build_amt_total / LS.real_amt_total * 100, 2 ), '%' ) lastmonth_build_proportion,
        | LSYM.epc_amt_total lastmonth_epc_amt_total,
        | CONCAT ( ROUND( LSYM.epc_amt_total / LS.real_amt_total * 100, 2 ), '%' ) lastmonth_epc_proportion
        |
        |FROM
        |   aj_dict_area AREA
        | LEFT JOIN
        |(
        |	SELECT
        |		county_name,
        |		SUM ( real_amt_total ) real_amt_total,
        |		SUM ( batch_total ) batch_total,
        |		SUM ( labourer_total ) labourer_total,
        |		SUM ( construction_amt_total ) construction_amt_total,
        |		SUM ( labour_amt_total ) labour_amt_total,
        |		SUM ( build_amt_total ) build_amt_total,
        |		SUM ( epc_amt_total ) epc_amt_total
        |	FROM
        |		labourer_salary_area_month
        |	GROUP BY
        |		county_name
        | ) LS ON AREA.name = LS.county_name
        |LEFT JOIN
        |(
        |	SELECT
        |		county_name,
        |   year,
        |   month,
        |		SUM ( real_amt_total ) real_amt_total,
        |		SUM ( batch_total ) batch_total,
        |		SUM ( labourer_total ) labourer_total,
        |		SUM ( construction_amt_total ) construction_amt_total,
        |		SUM ( labour_amt_total ) labour_amt_total,
        |  SUM ( build_amt_total ) build_amt_total,
        |		SUM ( epc_amt_total ) epc_amt_total
        |	FROM
        |		labourer_salary_area_month
        |	WHERE year = YEAR (add_months ( now(),- 1 ))
        |		AND month = MONTH (add_months ( now(),- 1 ))
        |	GROUP BY
        |		year,month,county_name
        |) LSYM ON AREA.name = LSYM.county_name
        |
        |""".stripMargin




    //保存数据到ads层
   // val dataFrame = spark.sql(sql)
//    dataFrame.show(100)

    //PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"labourer_salary_area_total")



    spark.stop()

  }

}

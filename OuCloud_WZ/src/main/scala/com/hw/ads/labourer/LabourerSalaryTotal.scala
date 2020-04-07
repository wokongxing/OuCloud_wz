package main.scala.com.hw.ads.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/** 需求:
 *    统计总发薪资信息:
 *      发薪金额,发薪批次数量,发放人数,企业发薪金额占比(分施工,劳务)、
 *      发薪项目数、发薪的竣工 在建项目数占比
 *
 *  数据来源:
 *      CDM层: labourer_salary_month 发薪聚合信息表
 *  条件: status =1,6
 *  获取字段:
 *      发薪总金额:--real_amt_total,
 * 	    发放批次数量:--batch_total,
 *      发放人数数量:--labourer_total,
 * 	    施工单位发放总金额:--construction_amt_total, (company-type=2 施工)
 *      施工单位发放金额占比:--construction_amt_proportion,
 * 	    劳务单位发放总金额:--labour_amt_total,  (company-type=3 劳务)
 *      劳务单位发放总金额占比:--labour_amt_proportion,
 *      薪资发放项目数:--project_total,
 *      在建项目数量:--build_project_total,
 *      在建项目占比:--build_project_proportion,
 *      竣工项目数量:--completed_project_total,
 *      竣工项目占比:--completed_project_proportion,
 *      统计时间:--create_time
 *
 * @author linzhy
 */
object LabourerSalaryTotal{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                  .appName(this.getClass.getSimpleName)
                  .master("local[2]")
                  .getOrCreate()

    var config = ConfigFactory.load()

    queryDataCreateView(spark,config)

    val sql =
      """
        |SELECT
        |	LAB.real_amt_total,
        |	LAB.batch_total,
        |	LAB.labourer_total,
        |	LAB.construction_amt_total,
        | CONCAT ( ROUND( LAB.construction_amt_total / LAB.real_amt_total * 100, 2 ), '%' ) construction_amt_proportion,
        |	LAB.labour_amt_total,
        | CONCAT ( ROUND( LAB.labour_amt_total / LAB.real_amt_total * 100, 2 ), '%' ) labour_amt_proportion,
        | PRO.project_total,
        | PRO.build_project_total,
        | CONCAT ( ROUND( PRO.build_project_total / PRO.project_total * 100, 2 ), '%' ) build_project_proportion,
        | PRO.completed_project_total,
        | CONCAT ( ROUND( PRO.completed_project_total / PRO.project_total * 100, 2 ), '%' ) completed_project_proportion,
        | now() create_time
        |FROM
        |	(
        |	SELECT
        |		1 AS LINKID,
        |		SUM ( real_amt ) real_amt_total,
        |		COUNT ( batch_no ) batch_total,
        |		SUM ( real_number ) labourer_total,
        |	  SUM ( CASE WHEN company_type = 2 THEN real_amt ELSE 0 END ) construction_amt_total,
        |	  SUM ( CASE WHEN company_type = 3 THEN real_amt ELSE 0 END ) labour_amt_total
        | FROM
        |	  labourer_salary_month
        | WHERE status=1 or status=6
        |	) LAB
        |	LEFT JOIN (
        |	SELECT
        |		1 AS LINKID,
        |		COUNT ( PRO.pid ) project_total,
        |	  SUM ( CASE WHEN PRO.project_condition = 1 THEN 1 ELSE 0 END ) build_project_total,
        |	  SUM ( CASE WHEN PRO.project_condition = 3 THEN 1 ELSE 0 END ) completed_project_total,
        |	  SUM ( CASE WHEN PRO.project_condition = 2 THEN 1 ELSE 0 END ) stop_project_total
        |FROM (
        |   SELECT
        |     pid,
        |     project_condition
        |   FROM
        |     labourer_salary_month
        |   WHERE status=1 or status=6
        |   GROUP BY pid, project_condition
        |  ) AS PRO
        |	) PRO ON LAB.LINKID = PRO.LINKID
        |""".stripMargin


    val dataFrame = spark.sql(sql)
//    dataFrame.show(100)
    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Append,dataFrame,config,"labourer_salary_total")

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

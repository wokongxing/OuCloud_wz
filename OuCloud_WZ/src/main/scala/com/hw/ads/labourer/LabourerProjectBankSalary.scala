package main.scala.com.hw.ads.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/** 需求:
 *    统计--pid项目发薪 参与的银行,以及涉及的金额;
 *    --注:pid有可能是源项目的子项目pid ,现在数据指标以pid为准
 *
 *  数据来源:
 *      CDM层: labourer_bank_area_month 员工发薪信息表
 *
 *  获取字段:
 *      项目名称:--project_name;
 *      银行名称:--real_amt_total,
 *      发薪人数总数:--labourer_total,
 *      发薪的金额:--project_total;
 *      排名:--rank;
 *
 * @author linzhy
 */
object LabourerProjectBankSalary{

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
        |	pid,
        |	project_name,
        |	county_name,
        |	bank_name,
        |	SUM ( ss_real_amt ) amt_total,
        |	SUM ( person_sum ) person_total ,
        |	row_number() over( partition by pid order by sum(ss_real_amt) desc ) rank
        |FROM
        |	labourer_salary_month
        |GROUP BY
        |	pid,
        |	project_name,
        |	county_name,
        |	bank_name
        |""".stripMargin


    val dataFrame = spark.sql(sql)

    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"labourer_project_bank_rank")

//    dataFrame.show(1000)

    spark.stop()

  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")

    //CDM层
    val labourer_salary_month = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "labourer_salary_month")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    labourer_salary_month.createOrReplaceTempView("labourer_salary_month")
  }
}

package main.scala.com.hw.ads.project

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**项目维度指标 项目状态-数据分析 -每天凌晨运行
 * 数据来源: CDM层
 *    project_day-以天为维度 -项目信息表;
 * 获取字段:
 *    区域,年月日,
 *    项目总数-project_total;
 *    在建项目数-bulid_total;
 *    竣工项目数-complete_total;
 *    停工项目数-stop_total;
 *    涉及企业数量-companys_sum;
 * @author lzhy
 */
object ProjectStatusDay {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()


    val sql=
      """
        |SELECT
        |	IFNULL( pd.county_name, '其他' ) county_name,
        |	pd.year,
        |	pd.month,
        |	pd.day,
        | pd.create_time,
        |	COUNT ( pd.pid ) project_total,
        |	SUM ( CASE WHEN project_condition = 1 THEN 1 ELSE 0 END ) AS bulid_total,
        |	SUM ( CASE WHEN project_condition = 2 THEN 1 ELSE 0 END ) AS stop_total,
        |	SUM ( CASE WHEN project_condition = 3 THEN 1 ELSE 0 END ) AS complete_total ,
        | COUNT( DISTINCT pd.cid ) AS companys_sum,
        | SUM ( pd.billboards ) billboards,
        |	SUM ( pd.project_account_status ) account_total
        |FROM
        |	project_day pd
        |GROUP BY
        |	pd.county_name,
        |	pd.year,
        |	pd.month,
        |	pd.day,
        | pd.create_time
        |""".stripMargin

    val dataFrame = spark.sql(sql)
    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"project_status_day")

    spark.stop();
  }

  //读取数据库数据
  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")

    //获取项目信息
    val project_day = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "project_day")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    project_day.createOrReplaceTempView("project_day")

  }
}

package main.scala.com.hw.ads.project

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**项目维度指标 在建项目类型-数据分析 -每天凌晨运行
 * 数据来源: CDM层
 *    project_day-以天为维度 -项目信息表;
 * 获取字段:
 *    区域,年月日,
 *    项目数-project_total;
 *    维权公示牌数--billboards;
 *    已实名项目数--realname_total;
 *    已备案专项账户项目数--account_total;
 *    已缴纳保障金项目数--safeguard_total;
 *    已实现云考勤项目数--attendance_total;
 *    已签订劳务合同项目数--contract_total;
 *    已签订专项监管三方协议项目数--agreement_total;
 *    项目类型--project_type(1-房屋;2-市政;3-附属;99-其他)
 * @author lzhy
 */
object ProjectBuildAreaTypeDay {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()

    queryDataCreateView(spark,config)

    val sql=
      """
        |	SELECT
        |		IFNULL( pd.county_name, '其他' ) county_name,
        |   pd.year,
        |   pd.month,
        |   pd.day,
        |   pd.create_time,
        |		COUNT ( pd.pid ) houses_total,
        |		SUM ( pd.billboards ) billboards,
        |		SUM ( pd.is_realname ) realname_total,
        |		SUM ( pd.project_account_status ) account_total,
        |  	SUM ( pd.safeguard_status ) safeguard_total,
        |		SUM ( pd.is_attendance ) attendance_total,
        |		SUM ( pd.contract_status ) contract_total,
        |		SUM ( pd.is_agreement ) agreement_total,
        |   '房屋建设工程' as project_type
        |FROM
        |	project_day pd
        |WHERE project_condition = 1
        |AND pd.project_type='房屋建设工程'
        |GROUP BY
        |	pd.county_name,
        | pd.year,
        | pd.month,
        | pd.day,
        | pd.create_time
        | UNION
        | SELECT
        |		IFNULL( pd.county_name, '其他' ) county_name,
        |   pd.year,
        |   pd.month,
        |   pd.day,
        |   pd.create_time,
        |		COUNT ( pd.pid ) municipal_total,
        |		SUM ( pd.billboards ) billboards,
        |		SUM ( pd.is_realname ) realname_total,
        |		SUM ( pd.project_account_status ) account_total,
        |  	SUM ( pd.safeguard_status ) safeguard_total,
        |		SUM ( pd.is_attendance ) attendance_total,
        |		SUM ( pd.contract_status ) contract_total,
        |		SUM ( pd.is_agreement ) agreement_total,
        |   '市政工程' as project_type
        |FROM
        |	project_day pd
        |WHERE project_condition = 1
        |AND pd.project_type='市政工程'
        |GROUP BY
        |	pd.county_name,
        | pd.year,
        | pd.month,
        | pd.day,
        | pd.create_time
        |  UNION
        | SELECT
        |		IFNULL( pd.county_name, '其他' ) county_name,
        |   pd.year,
        |   pd.month,
        |   pd.day,
        |   pd.create_time,
        |		COUNT ( pd.pid ) sub_total,
        |		SUM ( pd.billboards ) billboards,
        |		SUM ( pd.is_realname ) realname_total,
        |		SUM ( pd.project_account_status ) account_total,
        |  	SUM ( pd.safeguard_status ) safeguard_total,
        |		SUM ( pd.is_attendance ) attendance_total,
        |		SUM ( pd.contract_status ) contract_total,
        |		SUM ( pd.is_agreement ) agreement_total,
        |   '附属工程' as project_type
        |FROM
        |	project_day pd
        |WHERE project_condition = 1
        |AND pd.project_type='附属工程'
        |GROUP BY
        |	pd.county_name,
        | pd.year,
        | pd.month,
        | pd.day,
        | pd.create_time
        |   UNION
        | SELECT
        |		IFNULL( pd.county_name, '其他' ) county_name,
        |   pd.year,
        |   pd.month,
        |   pd.day,
        |   pd.create_time,
        |		COUNT ( pd.pid ) other_total,
        |		SUM ( pd.billboards ) billboards,
        |		SUM ( pd.is_realname ) realname_total,
        |		SUM ( pd.project_account_status ) account_total,
        |  	SUM ( pd.safeguard_status ) safeguard_total,
        |		SUM ( pd.is_attendance ) attendance_total,
        |		SUM ( pd.contract_status ) contract_total,
        |		SUM ( pd.is_agreement ) agreement_total,
        |   '其它' as project_type
        |FROM
        |	project_day pd
        |WHERE project_condition = 1
        |AND pd.project_type='其它'
        |GROUP BY
        |	pd.county_name,
        | pd.year,
        | pd.month,
        | pd.day,
        | pd.create_time
        |""".stripMargin

    //保存数据到ads层
    val dataFrame = spark.sql(sql)
    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"project_build_area_day")

    //spark.sql(sql).show(100)
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

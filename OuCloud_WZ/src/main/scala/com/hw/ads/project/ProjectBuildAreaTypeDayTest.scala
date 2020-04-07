package main.scala.com.hw.ads.project

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**项目维度指标 在建项目类型-数据分析 -每天凌晨运行
 * 数据来源: ADS层
 *    project_build_area_day-以天为维度 -项目信息表;
 *
 *  测试数据:
 *    依据区域、项目分类权限 获取相对应的数据,统计指标
 * @author lzhy
 */
object ProjectBuildAreaTypeDayTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()
    val ads_url = config.getString("pg.oucloud_ads.url")
    val ads_user = config.getString("pg.oucloud_ads.user")
    val ads_password = config.getString("pg.oucloud_ads.password")

    //获取项目信息
    val project_build_area_day = spark.read.format("jdbc")
      .option("url", ads_url)
      .option("dbtable", "project_build_area_day")
      .option("user", ads_user)
      .option("password", ads_password)
      .load()


    project_build_area_day.createOrReplaceTempView("project_build_area_day")

    val sql=
      """
        |SELECT
        |   pd.county_name,
        |		pd.year,
        |   pd.month,
        |   pd.day,
        |   pd.create_time,
        |   SUM ( pd.houses_total ) project_total,
        |   SUM ( case when pd.project_type=1 then pd.houses_total else 0 end ) houses_total,
        |   SUM ( case when pd.project_type=2 then pd.houses_total else 0 end ) municipal_total,
        |   SUM ( case when pd.project_type=3 then pd.houses_total else 0 end ) sub_total,
        |   SUM ( case when pd.project_type=99 then pd.houses_total else 0 end ) other_total,
        |   SUM ( pd.billboards ) billboards,
        |		SUM ( pd.realname_total ) realname_total,
        |		SUM ( pd.account_total ) account_total,
        |  	SUM ( pd.safeguard_total ) safeguard_total,
        |		SUM ( pd.attendance_total ) attendance_total,
        |		SUM ( pd.contract_total ) contract_total,
        |		SUM ( pd.agreement_total ) agreement_total
        |FROM
        |	project_build_area_day pd
        |GROUP BY
        |	pd.county_name,
        | pd.year,
        | pd.month,
        | pd.day,
        | pd.create_time
        |
        |""".stripMargin

    //保存数据到ads层
//    spark.sql(sql).write.format("jdbc")
//      .mode(SaveMode.Overwrite)
//      .option("dbtable","project_build_area_day")
//      .option("url",ads_url)
//      .option("user",ads_user)
//      .option("password",ads_password)
//      .save()
    spark.sql(sql).show(100)
    spark.stop();
  }
}

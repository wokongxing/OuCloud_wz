package main.scala.com.hw.ads.project

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**项目维度指标 在建项目类型总计-数据分析 -每天凌晨运行
 * 数据来源: CDM层
 *    project_day-以天为维度 -项目信息表;
 *    项目类型-(1-房屋;2-市政;3-附属;99-其他)
 * 获取字段:
 *    区域;
 *    在建项目数-bulid_total;
 *    市政项目数--municipal_total;
 *    房屋项目数--houses_total;
 *    附属项目数--sub_total;
 *    其他项目数--other_total;
 *    维权公示牌数--billboards;
 *    已实名项目数--realname_total;
 *    已备案专项账户项目数--account_total;
 *    已缴纳保障金项目数--safeguard_total;
 *    已实现云考勤项目数--attendance_total;
 *    已签订劳务合同项目数--contract_total;
 *    已签订专项监管三方协议项目数--agreement_total;
 *    以及各个所占比率;
 * @author lzhy
 */
object ProjectBuildTotal {
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
    val project_build_day = spark.read.format("jdbc")
      .option("url", ads_url)
      .option("dbtable", "project_build_day")
      .option("user", ads_user)
      .option("password", ads_password)
      .load()

    project_build_day.createOrReplaceTempView("project_build_day")

    val sql=
      """
        |SELECT
        |	P.province_name,
        |	P.city_name,
        |	P.county_name,
        |	P.project_total,
        |	P.houses_total,
        |	concat ( round( P.houses_total / P.project_total * 100, 2 ), '%' ) houses_proportion,
        |	P.municipal_total,
        |	concat ( round( P.municipal_total / P.project_total * 100, 2 ), '%' ) municipal_proportion,
        |	P.sub_total,
        |	concat ( round( P.sub_total / P.project_total * 100, 2 ), '%' ) sub_proportion,
        |	P.other_total,
        |	concat ( round( P.other_total / P.project_total * 100, 2 ), '%' ) other_proportion,
        |	P.billboards,
        |	concat ( round( P.billboards / P.project_total * 100, 2 ), '%' ) billboards_proportion,
        |	P.realname_total,
        |	concat ( round( P.realname_total / P.project_total * 100, 2 ), '%' ) realname_proportion,
        |	P.account_total,
        |	concat ( round( P.account_total / P.project_total * 100, 2 ), '%' ) account_proportion,
        | P.safeguard_total,
        |	concat ( round( P.safeguard_total / P.project_total * 100, 2 ), '%' ) safeguard_proportion,
        |	P.attendance_total,
        |	concat ( round( P.attendance_total / P.project_total * 100, 2 ), '%' ) attendance_proportion,
        |	P.contract_total,
        |	concat ( round( P.contract_total / P.project_total * 100, 2 ), '%' ) contract_proportion,
        |	P.agreement_total,
        |	concat ( round( P.agreement_total / P.project_total * 100, 2 ), '%' ) agreement_proportion
        |FROM
        |	(
        |	SELECT
        |		pd.county_name,
        |   pd.city_name,
        |   pd.province_name,
        |		SUM ( pd.project_total ) project_total,
        |		SUM ( pd.houses_total ) houses_total,
        |		SUM ( pd.municipal_total ) municipal_total,
        |		SUM ( pd.sub_total) sub_total,
        |		SUM ( pd.other_total) other_total,
        |		SUM ( pd.billboards ) billboards,
        |		SUM ( pd.realname_total ) realname_total,
        |		SUM ( pd.account_total ) account_total,
        |  	SUM ( pd.safeguard_total ) safeguard_total,
        |		SUM ( pd.attendance_total ) attendance_total,
        |		SUM ( pd.contract_total ) contract_total,
        |		SUM ( pd.agreement_total ) agreement_total
        |FROM
        |	project_build_day pd
        |GROUP BY
        |	pd.county_name,
        | pd.city_name,
        | pd.province_name
        |	) P
        |""".stripMargin


    //保存数据到ads层
    spark.sql(sql).write.format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("dbtable","project_build_total")
      .option("url",ads_url)
      .option("user",ads_user)
      .option("password",ads_password)
      .save()

//    spark.sql(sql).show(100)
    spark.stop();
  }


}

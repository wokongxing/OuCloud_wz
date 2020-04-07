package main.scala.com.hw.ads.project

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 数据来源:
 *       CDM层 project_day-以天为维度 -项目信息表;
 *       gx_salary_batch--发放薪资批次主表;
 *       gx_labourer--民工表;
 *       gx_company--工薪企业;
 * 获取字段:
 *    '温州' as city_name,
 *    项目总数-project_total;
 *    在建项目数-bulid_total(project_status = 3);
 *    竣工项目数-complete_total(project_status = 2);
 *    停工项目数-stop_total(project_status = 1);
 *    涉及企业数量-companys_sum;
 *    已缴纳保证金企业--is_ompanys_safeguard;
 *    无缴纳保证金企业数--no_ompanys_safeguard
 *    保证金缴纳总额--assure_amt;
 *    已实名制民工人数--realname_total;
 *    已发薪资总金额--real_amt;
 *    劳务费用专项账户开户数--account_total;
 *    维权公示牌数--billboards;
 *
 * @author lzhy
 */
object ProjectTotal {
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
        |SELECT
        |   '温州' as city_name,
        |   now() as create_time,
        |	  A1.project_total,
        | 	A1.bulid_total,
        | 	A1.complete_total,
        | 	A1.stop_total,
        | 	A1.billboards,
        |   A1.real_amt,
        |  	A2.is_ompanys_safeguard,
        | 	A2.no_ompanys_safeguard,
        | 	A3.realname_total,
        |   A4.companys_total
        |FROM
        |	(
        |	SELECT
        |		1 AS ID,
        |		COUNT ( pd.pid ) project_total,
        |		SUM ( CASE WHEN project_condition = 1 THEN 1 ELSE 0 END ) AS bulid_total,
        |		SUM ( CASE WHEN project_condition = 3 THEN 1 ELSE 0 END ) AS complete_total,
        |		SUM ( CASE WHEN project_condition = 2 THEN 1 ELSE 0 END ) AS stop_total,
        |		SUM ( pd.billboards ) billboards,
        |		SUM ( pd.real_amt ) real_amt
        |	FROM
        |		project_day pd
        | where city_code='330300'
        | and is_deleted=1 and status in (3,4,5)
        | and response_code !='' and create_user !=''
        |	) A1
        |	LEFT JOIN (
        |	  SELECT
        |		  1 AS ID,
        |		  SUM ( PS.assure_amt ) assure_amt,
        |		  SUM ( CASE WHEN PS.safeguard_status = 1 THEN 1 ELSE 0 END ) is_ompanys_safeguard,
        |		  SUM ( CASE WHEN PS.safeguard_status = 0 THEN 1 ELSE 0 END ) no_ompanys_safeguard
        |   FROM
        |	  (
        |	    SELECT
        |		    pd.cid,
        |		    pd.assure_amt,
        |		    pd.safeguard_status
        |	    FROM
        |		    project_day pd
        |	    GROUP BY
        |		  pd.cid,
        |		  pd.assure_amt,
        |		  safeguard_status
        |	  ) PS
        |	) A2 ON A1.ID = A2.ID
        | LEFT JOIN (
        |   SELECT
        |     1 AS ID,
        |     COUNT(*) AS realname_total
        |   FROM
        | 	  gx_labourer l
        |   WHERE l.real_grade != 99
        |   AND l.is_deleted =1
        | ) A3 ON A1.ID = A3.ID
        | LEFT JOIN (
        |   SELECT
        |     1 AS ID,
        |     COUNT(*) companys_total
        |   from
        |     gx_company
        |   WHERE is_deleted=1
        |   and businesslicense_code !=''
        |   AND status !='4'
        | ) A4 ON A1.ID = A4.ID
        |""".stripMargin


    //保存数据到ads层
    val dataFrame = spark.sql(sql)
    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Append,dataFrame,config,"project_total")

 // spark.sql(sql).show(100)


    spark.stop();
  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")
    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")
    //获取项目信息
    val project_day = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "project_day")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    //劳务人员
    val gx_labourer = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_labourer")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    //工薪企业
    val gx_company = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_company")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    project_day.createOrReplaceTempView("project_day")
    gx_labourer.createOrReplaceTempView("gx_labourer")
    gx_company.createOrReplaceTempView("gx_company")

  }
}

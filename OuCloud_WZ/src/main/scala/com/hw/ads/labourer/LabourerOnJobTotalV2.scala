package main.scala.com.hw.ads.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**仅统计了 在职员工数据
 *
 *数据来源: ODS层
 *  gx_company_labourer--项目民工表;
 *  aj_dict_area--区域表;
 *  gx_mig_arrears_labour--欠薪员工表;
 *  gx_mig_arrears_salary  --项目欠薪表;
 *
 * 获取字段:
 *   lid--民工唯一id;
 *   gender--性别,
 *   birthday--出生日期:年月日,
 *   age--民工年龄,
 *   real_name--民工真实名字,
 * 	 job_date--入职时间,
 * 	 current_city_code--当前所在城市,
 * 	 current_county_code--当前所在区域编码,
 * 	 current_county_name--当前所在区域名称,
 *   create_time--数据创建时间,
 * 	 worktype_no--当前所做工种的编码,
 * 	 worktype_name--当前所做工种的名称,
 *   is_job--就职状态(1--在职,0--离职),
 *   real_amt--欠薪资金金额,
 *   arrears_status--欠薪状态(1--欠薪,0--未欠薪)
 *
 * @author linzhy
 */
object LabourerOnJobTotalV2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .config("spark.sql.crossJoin.enabled","true")
      .getOrCreate()

    var config = ConfigFactory.load()

    queryDataCreateView(spark,config)

    val sql=
      """
        |with salary_lab as (
        |SELECT
        |	mas.city_code,
        |	mas.city_name,
        |	mas.county_code,
        |	mas.county_name,
        |	count(*) arrears_total,
        |	sum (mal.real_amt) real_amt
        |FROM
        |	(
        |	SELECT
        |		mas.city_code,
        |		mas.city_name,
        |		mas.county_code,
        |		mas.county_name,
        |		mas.id,
        |		mas.pid
        |	FROM
        |		gx_mig_arrears_salary mas
        |	WHERE
        |		mas.STATUS = 3
        |		AND mas.is_deleted = 1
        |		and pid!=0
        |	) mas
        |	JOIN (
        |	SELECT
        |		lid,pid,
        |		arrears_salary_id,
        |		sum( real_amt ) real_amt
        |	FROM
        |		gx_mig_arrears_labour mal
        |	WHERE
        |		mal.is_deleted = 1
        |	GROUP BY lid,arrears_salary_id ,pid
        |
        |) mal ON mas.id = mal.arrears_salary_id
        |group by city_code,city_name,county_code,county_name
        |),
        | on_job as (
        |SELECT
        |	cl.current_city_code,
        |	cl.current_county_code,
        |	cl.current_county_name ,
        |	count(*) labourer_total
        |FROM
        |	(
        |	SELECT
        |		lid,
        |		real_name,
        |		job_date,
        |		current_city_code,
        |		current_county_code,
        |		current_county_name,
        |		create_time,
        |		worktype_no,
        |		worktype_name,
        |		is_job,
        |		ROW_NUMBER () OVER ( PARTITION BY lid ORDER BY create_time DESC NULLS LAST ) top
        |	FROM
        |		gx_company_labourer
        |	WHERE
        |		is_deleted = 1
        |		AND is_job = '1'
        |		AND current_city_code = '330300'
        |	) cl WHERE top = 1
        | GROUP BY
        |	cl.current_city_code,
        |	cl.current_county_code,
        |	cl.current_county_name
        |),
        | area as (
        | select
        |   1 as linkid,
        |   area.name,
        |   area.code
        | from
        |  aj_dict_area area
        | where parent_code='330300'
        | and level=3
        | )
        |
        | select
        |   area.name county_name,
        |   la.labourer_total,
        |   on_job.labourer_total onjob_labourer_total,
        |   lab1.arrears_total,
        |   lab2.county_arrears_total
        | from
        |   area
        | LEFT JOIN (
        |   select
        |     1 as linkid,
        |     sum (arrears_total) arrears_total
        |   from
        |     salary_lab
        | ) lab1 ON area.linkid = lab1.linkid
        | left join (
        |   select
        |     county_code,
        |     arrears_total as county_arrears_total
        |   from
        |     salary_lab where city_code=330300
        | ) lab2  ON area.code = lab2.county_code
        | LEFT JOIN on_job ON area.CODE = on_job.current_county_code
        | LEFT JOIN (
        |   SELECT
        |     1 as linkid,
        |     count(*) labourer_total
        |   FROM
        | 	  gx_labourer l
        |   WHERE l.real_grade != 99
        |   AND l.is_deleted =1
        | ) la on area.linkid = la.linkid
        |""".stripMargin


    val dataFrame = spark.sql(sql)
    //dataFrame.show(100)
    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"labourer_onjob_total")

    spark.stop()

  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    //项目民工表
    val gx_company_labourer = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_company_labourer")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    //劳务人员
    val gx_labourer = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_labourer")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    val gx_mig_arrears_salary = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_mig_arrears_salary")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //欠薪员工表
    val gx_mig_arrears_labour = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_mig_arrears_labour")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //获取区域字典
    val aj_dict_area = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","aj_dict_area")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    gx_mig_arrears_salary.createOrReplaceTempView("gx_mig_arrears_salary")
    gx_company_labourer.createOrReplaceTempView("gx_company_labourer")
    gx_mig_arrears_labour.createOrReplaceTempView("gx_mig_arrears_labour")
    gx_labourer.createOrReplaceTempView("gx_labourer")
    aj_dict_area.createOrReplaceTempView("aj_dict_area")

  }
}

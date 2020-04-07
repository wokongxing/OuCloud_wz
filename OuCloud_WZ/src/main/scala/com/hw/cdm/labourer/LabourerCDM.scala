package main.scala.com.hw.cdm.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**仅统计了 在职员工数据
 *
 *数据来源: ODS层
 *  gx_company_labourer--项目民工表;
 *  gx_labourer--劳务人员表;
 *  gx_mig_arrears_labour--欠薪员工表;
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
object LabourerCDM {
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
        |	GL.lid,
        | GL.gender,
        | DATE_FORMAT(GL.birthday,'Y-M-d') birthday,
        | YEAR(now()) - YEAR(GL.birthday) as age,
        | GL.real_name,
        |	CL.job_date,
        |	CL.current_city_code,
        |	CL.current_county_code,
        |	CL.current_county_name,
        | CL.create_time,
        |	CL.worktype_no,
        |	CL.worktype_name,
        | case when CL.is_job is null or CL.is_job=false then 0 else 1 end as is_job,
        | mal.real_amt,
        | CASE WHEN mal.lid is null THEN 0 ELSE 1 END AS arrears_status
        |FROM
        |	gx_labourer GL
        |LEFT JOIN (
        |	SELECT
        |		*
        |	FROM
        |		(
        |		SELECT
        |			id,
        |			lid,
        |			real_name,
        |			job_date,
        |			current_city_code,
        |			current_county_code,
        |			current_county_name,
        |			create_time,
        |			worktype_no,
        |			worktype_name,
        |			is_job,
        |			row_number() over( PARTITION BY lid ORDER BY create_time ASC nulls last ) top
        |		FROM
        |			gx_company_labourer
        |		WHERE is_deleted = 1
        |		AND is_job = 1
        |		)
        |	WHERE top = 1
        |) CL ON GL.lid = CL.lid where GL.is_deleted = 1
        |LEFT JOIN  gx_mig_arrears_labour mal ON GL.lid=mal.lid
        | and mal.is_deleted= 1
        |""".stripMargin


    val dataFrame = spark.sql(sql)
    PgSqlUtil.saveDataToPgsqlCdm(SaveMode.Overwrite,dataFrame,config,"labourer_day")

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
    //欠薪员工表
    val gx_mig_arrears_labour = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_mig_arrears_labour")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    gx_labourer.createOrReplaceTempView("gx_labourer")
    gx_company_labourer.createOrReplaceTempView("gx_company_labourer")
    gx_mig_arrears_labour.createOrReplaceTempView("gx_mig_arrears_labour")


  }
}

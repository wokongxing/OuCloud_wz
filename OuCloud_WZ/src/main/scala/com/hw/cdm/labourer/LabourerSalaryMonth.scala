package main.scala.com.hw.cdm.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**需求:统计劳务人员的工资发放情况 月
 *  数据来源: ODS层
 *      gx_salary_batch -- 工薪工资发放批次主表; 条件:type=1(工资类型,项目工资) and status=1 (发放成功的数据)
 *      gx_bank_account -- 工薪银行账户表;
 *      aj_companys -- 安监企业信息表;
 *      aj_projects -- 安监项目信息表;
 *  获取字段信息:
 *      批次号: --batch_no;
 *      企业唯一id:--cid;
 *      源项目唯一id:--source_pid;
 *      项目唯一id: --pid;
 *      映射银行账户唯一标识:--bkid;
 *      年份:--year;
 *      月份:--month;
 *      年月时间:--year_month;
 *      发薪真实人数:--real_number;
 *      发薪真实金额:--real_amt;
 *      公司名称:--company_name;
 *      公司类型:--company_type;(13--EPC,1--建设,2--设计,4--施工,11--劳务)
 *      项目开展情况:--project_condition; (1：正常，2：停工，3：竣工)
 *      项目所在的区域: --county_code;
 *      银行名称: --bank_name;
 *
 * @author linzhy
 */
object LabourerSalaryMonth {
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
        |select
        | GSB.batch_no,
        | GSB.cid,
        | GSB.source_pid,
        | GSB.pid,
        | GSB.bkid,
        | GSB.year,
        | GSB.month,
        | GSB.year_month,
        | GSB.real_number,
        | GSB.real_amt,
        | AC.company_name,
        | AC.company_type,
        | AP.project_name,
        | AP.county_code,
        | AP.project_condition,
        | BA.bank_name
        |from (
        | SELECT
        |	  batch_no,
        |	  cid,
        |	  source_pid,
        |	  pid,
        |	  bkid,
        |	  year,
        |	  month,
        |	  year_month,
        |	  real_number,
        |	  real_amt
        | FROM
        |	  gx_salary_batch
        | WHERE status = 1
        |	AND TYPE =1
        | ) GSB
        | LEFT JOIN aj_companys AC ON GSB.cid = AC.cid
        | LEFT JOIN gx_bank_account BA ON GSB.bkid = BA.bkid
        | LEFT JOIN (
        | SELECT
        |	  gp.pid,
        |   gp.project_name,
        |	  gp.county_code,
        |	  gp.project_condition
        |FROM
        |	  gx_project gp
        |UNION
        |SELECT
        |	  gps.pid,
        |   gps.project_name,
        |	  gps.county_code,
        |   gps.project_condition
        |FROM
        |	  gx_project_sub gps
        |  ) AP ON GSB.pid = AP.pid
        |
        |""".stripMargin


    val dataFrame = spark.sql(sql)
//    PgSqlUtil.saveDataToPgsqlCdm(SaveMode.Overwrite,dataFrame,config,"labourer_salary_month")

    dataFrame.show(10000)


    spark.stop();
  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    //工薪工资发放批次主表
    val gx_salary_batch = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_salary_batch")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    //工薪银行账户表
    val gx_bank_account = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_bank_account")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //安监企业信息表
    val aj_companys = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "aj_companys")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //工薪项目信息表
    val gx_project_sub = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_project_sub")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //工薪项目主表
    val gx_project = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_project")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    gx_salary_batch.createOrReplaceTempView("gx_salary_batch")
    gx_bank_account.createOrReplaceTempView("gx_bank_account")
    aj_companys.createOrReplaceTempView("aj_companys")
    gx_project.createOrReplaceTempView("gx_project")
    gx_project_sub.createOrReplaceTempView("gx_project_sub")

  }
}

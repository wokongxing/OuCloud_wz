package main.scala.com.hw.test

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
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
object LabourerDayV2Test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()
    val cdm_url = config.getString("db.sps.url")
    val cdm_user = config.getString("db.sps.user")
    val cdm_password = config.getString("db.sps.password")

    //项目民工表
    val bank_transfer = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "bank_transfer")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    val project = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "project")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    bank_transfer.createOrReplaceTempView("ods_qx_bank_transfer")
    project.createOrReplaceTempView("project")

    val datetime="2020-02-01"
    val datetime2="-3"
    val sql=
      s"""
        |SELECT
        |   concat(qp.pid,year('${datetime}'),month('${datetime}')) id,
        |	 qp.pid,
        |	 ifnull(pb.status,1) status,
        |   trunc('${datetime}','MM')  cur_date,
        |   year('${datetime}')  cur_date_year,
        |   month('${datetime}') cur_date_month,
        |   ifnull(pb.amt,0) amt,
        |   '' as create_user,
        |   now() as create_time,
        |   '' as last_modify_user,
        |   now() as last_modify_time,
        |   1 as is_deleted,
        |   year(add_months(now(),${datetime2})),
        |   month(add_months(now(),${datetime2}))
        |from
        |  project qp
        |left join (
        |   SELECT
        |      pid,
        |      case when SUM(amt)>0 then 3 else 1 end status,
        |	    SUM(amt) amt
        |    FROM
        |	    ods_qx_bank_transfer
        |    WHERE jtype = 1
        |    AND is_deleted = 1
        |    and year(trans_time)=year('${datetime}')
        |    and month(trans_time)= month('${datetime}')
        |    GROUP BY pid
        |) pb on qp.pid = pb.pid
        |""".stripMargin


    spark.sql(sql).show()


    spark.stop();
  }
}

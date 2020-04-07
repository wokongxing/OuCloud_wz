package main.scala.com.hw.cdm.companys

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 执行时间:每天凌晨执行一次,覆盖
 * 企业数据join
 * --数据来源:OuCloud_ODS 层
 * --表:gx_company --工薪企业表,
 *      sys_area,
 *      dim_gx_company_type, --企业类型
 *      gx_project --工薪项目表
 * --数据关联:
 *    companys.cid:projects.cid;
 *    companys.company_type:dict_simple.code(dict.type='companytype')
 *    companys.location:dict_simple.code(dict_simple.type='company_resource')
 *  --dict_area:
 *    level: 1--省;2--市;3--区
 *    companys.province_code:area.code
 * 获取字段:
 *    企业cid,
 *    名称
 *    行政区划-省市区,
 *     company_type --企业类型,--1：建设集团，2：施工单位，3：劳务企业，4：设计单位
 *     create_time --创建时间,
 *     checked_time --审批时间,
 *     register_capital --注册资金,
 *     is_location --是否本地企业(1-本地,2-外地,),
 *     is_deleted --是否删除(1-删除 2-未删除),
 *     status --审批状态(1：已保存 2：待审批 3：审批通过 4：审批拒绝),
 *    企业入驻安监时间-年月日,
 *    企业业务管辖(即项目区划,省市区),
 *
 * @author lzhy
 */
object CompanyDayGX {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    //获取配置参数 信息
    val config = ConfigFactory.load()

    queryDataCreateView(spark,config)

    // 企业聚合 获取 企业的行政区域 业务区域 来源 企业类型
    val sql =
      """
        |SELECT
        | com.cid,
        | com.company_name,
        | com.province_code,
        | com.city_code,
        | com.county_code,
        | com.company_type,
        | sim.name company_type_name,
        | com.create_time,
        | com.checked_time,
        | YEAR(com.create_time) year,
        |	MONTH(com.create_time) month,
        | DAY(com.create_time) day,
        | com.register_capital,
        | com.is_location,
        | com.is_deleted,
        | com.status,
        | pro.province_code project_province_code,
        | pro.city_code project_city_code,
        | pro.county_code project_county_code,
        | row_number() over( partition by pro.cid order by pro.cid asc ) rank
        |FROM
        |	gx_company com
        | left join dim_gx_company_type sim on com.company_type=sim.code
        | left join (
        |  select
        |   pro.cid,
        |   pro.province_code,
        |   pro.city_code,
        |   pro.county_code
        | from
        |   gx_project pro
        | where pro.status in (3,4,5)
        | and  pro.is_deleted=1
        | and  pro.project_condition=1
        | group by pro.cid,pro.province_code,pro.city_code,pro.county_code
        | ) pro on com.cid = pro.cid
        |
        |""".stripMargin


    val dataFrame = spark.sql(sql)
//    dataFrame.show(1000)
    PgSqlUtil.saveDataToPgsqlCdm(SaveMode.Overwrite,dataFrame,config,"cdm_companys")

    spark.stop()

  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val url = config.getString("pg.oucloud_ods.url")
    val user = config.getString("pg.oucloud_ods.user")
    val password = config.getString("pg.oucloud_ods.password")

    //获取company数据
    val gx_company = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable","gx_company")
      .option("user", user)
      .option("password", password)
      .load()
    //获取简单字典
    val dim_gx_company_type = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable","dim_gx_company_type")
      .option("user", user)
      .option("password", password)
      .load()
    //获取区域字典
    val aj_dict_area = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable","aj_dict_area")
      .option("user", user)
      .option("password", password)
      .load()

    //获取项目数据
    val gx_project = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable","gx_project")
      .option("user", user)
      .option("password", password)
      .load()

    gx_company.where("is_deleted=1 and status=3").createOrReplaceTempView("gx_company")
    dim_gx_company_type.createOrReplaceTempView("dim_gx_company_type")
    aj_dict_area.createOrReplaceTempView("aj_dict_area")
    gx_project.createOrReplaceTempView("gx_project")

  }
}

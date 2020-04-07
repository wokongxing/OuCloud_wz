package  main.scala.com.hw.cdm.companys

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 执行时间:每天凌晨执行一次,覆盖
 * 企业数据join
 * --数据来源:OuCloud_ODS 层
 * --表:companys,sys_area,sys_simple,projects
 * --数据关联:
 *    companys.cid:projects.cid;
 *    companys.company_type:dict_simple.code(dict.type='companytype')
 *    companys.location:dict_simple.code(dict_simple.type='company_resource')
 *  --dict_area:
 *    level: 1--省;2--市;3--区
 *    companys.province_code:area.code
 * 获取字段:
 *    企业cid,名称 行政区划-省市区,企业类型,来源,
 *    企业入驻安监时间-年月日,企业业务管辖(即项目区划,省市区),
 * @author lzhy
 */
object CompanyDay {

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
        | area.name province_name,
        | area2.name city_name,
        | area3.name county_name,
        | sim.name company_type,
        | com.location,
        | YEAR(com.create_time) year,
        |	MONTH(com.create_time) month,
        | DAY(com.create_time) day,
        | area4.name project_province_name,
        | area5.name project_city_name,
        | area6.name project_county_name,
        | now() create_time
        |FROM
        |	aj_companys com
        | left join aj_dict_simple sim on sim.type='companytype' and com.company_type=sim.code
        | left join aj_dict_area area on area.level=1 and com.province_code=area.code
        | left join aj_dict_area area2 on area2.level=2 and com.city_code=area2.code
        | left join aj_dict_area area3 on area3.level=3 and com.county_code=area3.code
        | left join (
        |  select
        |   pro.cid,
        |   pro.province_code,
        |   pro.city_code,
        |   pro.county_code
        | from
        |   aj_projects pro
        | where pro.is_completed='N'
        | and pro.status=3
        | group by pro.cid,pro.province_code,pro.city_code,pro.county_code
        | ) pro on com.cid = pro.cid
        | left join aj_dict_area area4 on area4.level=1 and pro.province_code=area4.code
        | left join aj_dict_area area5 on area5.level=2 and pro.city_code=area5.code
        | left join aj_dict_area area6 on area6.level=3 and pro.county_code=area6.code
        |""".stripMargin


    val dataFrame = spark.sql(sql)
    PgSqlUtil.saveDataToPgsqlCdm(SaveMode.Overwrite,dataFrame,config,"cdm_companys")

    spark.stop()

  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val url = config.getString("pg.oucloud_ods.url")
    val user = config.getString("pg.oucloud_ods.user")
    val password = config.getString("pg.oucloud_ods.password")

    //获取company数据
    val aj_companys = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable","aj_companys")
      .option("user", user)
      .option("password", password)
      .load()
    //获取简单字典
    val aj_dict_simple = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable","aj_dict_simple")
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
    val aj_projects = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable","aj_projects")
      .option("user", user)
      .option("password", password)
      .load()

    aj_companys.where("is_deleted=1 and status=3").createOrReplaceTempView("aj_companys")
    aj_dict_simple.createOrReplaceTempView("aj_dict_simple")
    aj_dict_area.createOrReplaceTempView("aj_dict_area")
    aj_projects.createOrReplaceTempView("aj_projects")

  }
}

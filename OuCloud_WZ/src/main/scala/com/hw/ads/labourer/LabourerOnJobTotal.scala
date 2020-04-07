package main.scala.com.hw.ads.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 数据来源:
 *   CDM层
 *      labourer_day_v2-民工信息表;
 *   ODS层:
 *      aj_dict_area 字典表
 * 获取字段:
 *    区域: --county_name;
 *    在职民工人数:--person_total,
 *    在职民工欠薪人数:--arrears_total;
 *    在职民工未欠薪人数:--no_arrears_total;
 *    区域民工在职人数: --county_person_total;
 *    区域民工在职人数占比: --person_proportion;
 *    区域在职民工欠薪人数:--county_arrears_total;
 *    区域在职民工欠薪人数占比:--arrears_proportion;
 *    区域在职民工未欠薪人数:--county_no_arrears_total;
 *    区域在职民工未欠薪人数占比:--no_arrears_proportion;
 *
 * @author lzhy
 */
object LabourerOnJobTotal {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()
    //获取数据,创建视图
    queryDataCreateView(spark,config)

    val sql=
      """
        |select
        |   area.code,
        |   area.name county_name,
        |   lab.person_total,
        |   lab.arrears_total,
        |   lab.no_arrears_total,
        |   lab.county_person_total,
        |   concat ( round(lab.county_person_total / lab.person_total * 100, 2 ), '%' ) person_proportion,
        |   lab.county_arrears_total,
        |   concat ( round(lab.county_arrears_total / lab.arrears_total * 100, 2 ), '%' ) arrears_proportion,
        |   lab.county_no_arrears_total,
        |   concat ( round(lab.county_no_arrears_total / lab.no_arrears_total * 100, 2 ), '%' ) no_arrears_proportion
        |from
        |   aj_dict_area area
        |LEFT JOIN (
        | select
        |   *
        | from
        |(
        | SELECT
        |   1 as linkid,
        |	  ld.current_county_code,
        |	  ld.area_county_name,
        |	  count(lid) county_person_total,
        |	  sum (case when ld.arrears_status=1 then 1 else 0 end ) county_arrears_total,
        |	  sum (case when ld.arrears_status=0 then 1 else 0 end ) county_no_arrears_total
        | FROM
        |	  labourer_day_v2 ld
        | where ld.top=1
        | and ld.real_grade !=99
        | and ld.is_job=1
        | and ld.is_deleted=1
        | group by ld.current_county_code,ld.area_county_name
        | ) lab1
        | LEFT JOIN (
        |   SELECT
        |	    1 as linkid,
        |	    count(lid) person_total,
        |	    sum (case when ld.arrears_status=1 then 1 else 0 end ) arrears_total,
        |	    sum (case when ld.arrears_status=0 then 1 else 0 end ) no_arrears_total
        |   FROM
        |	    labourer_day_v2 ld
        |   where ld.top=1
        |	  and ld.real_grade !=99
        |) lab2 on lab1.linkid = lab2.linkid
        | ) lab on area.code = lab.current_county_code
        |
        |""".stripMargin


    val dataFrame = spark.sql(sql)
    dataFrame.show(1000)
    //PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"labourer_onjob_total")


    spark.stop();
  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    //获取劳务员工聚合信息
    val labourer_day_v2 = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "labourer_day_v2")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    //获取区域字典
    val aj_dict_area = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","aj_dict_area")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    labourer_day_v2.where("top=1 and is_job=1 and is_deleted=1 and current_city_code='330300'")createOrReplaceTempView("labourer_day_v2")
    aj_dict_area.where("parent_code='330300' and level=3")createOrReplaceTempView("aj_dict_area")

  }
}

package  main.scala.com.hw.ads.companys

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 统计企业维度指标,每天的增量
 * --数据来源:
 *    oucloud_ods----aj_companys;
 *    cdm层--companys;
 * 企业类型--company_type: 1：建设集团，2：施工单位，3：劳务企业，4：设计单位
 * 企业来源--location:1--本地;2--外地
 * 按区域划分:管辖内 行政企业 业务企业(业务本地,业务外地,业务其他) 数量以及占比
 * 行政企业: 依据--county_name
 * 业务企业: 依据--project_county_name
 * @author linzhy
 */
object CompanysDay {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ADS_Companys")
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()
    queryDataCreateView(spark,config)

    //施工、劳务 epc /本地 外地 其他 占比

    val sql =
      """
        | with xz_com as (
        |  SELECT
        |   com.county_code,
        |   YEAR(com.create_time) year,
        | 	MONTH(com.create_time) month,
        |   DAY(com.create_time) day,
        |	  count(com.cid) xz_companys_sum,
        |	  sum(case when com.company_type_name='设计单位' then 1 else 0 end) EPC_count,
        |	  sum(case when com.company_type_name='施工单位' then 1 else 0 end) company_construction_count,
        |	  sum(case when com.company_type_name='劳务企业' then 1 else 0 end) company_labour_count,
        |	  sum(case when com.company_type_name='建设集团' then 1 else 0 end) company_build_count,
        |	  sum(case when com.is_location=1 then 1 else 0 end) xz_local_count,
        |	  sum(case when com.is_location=2 then 1 else 0 end) xz_nonlocal_count,
        |	  sum(case when com.is_location=2 or com.is_location=1 then 0 else 1 end) xz_other
        |	from
        |	  cdm_companys com
        | where com.is_deleted=1 and status=3 and rank=1 and com.county_code is not null
        | group by com.county_code, YEAR(com.create_time),
        |	   MONTH(com.create_time),DAY(com.create_time)
        | ),
        | yw_com as (
        |  SELECT
        |	  com.project_county_code,
        |   com.year,
        |   com.month,
        |   com.day,
        |	  count(com.cid) yw_companys_sum,
        |	  SUM	( CASE WHEN com.is_location = 1 THEN 1 ELSE 0 END ) yw_local_count,
        |	  SUM ( CASE WHEN com.is_location = 2 THEN 1 ELSE 0 END ) yw_nonlocal_count,
        |	  SUM ( CASE WHEN com.is_location = 2 OR com.is_location = 1 THEN 0 ELSE 1 END ) yw_other
        | FROM
        |	  cdm_companys com
        | WHERE com.project_county_code is not null
        | and project_city_code = '330300'
        | GROUP BY com.project_county_code,
        |         com.year,
        |         com.month,com.day
        | )
        | select
        | area.code county_code,
        | area.name county_name,
        | coms.year,
        | coms.month,
        | coms.day,
        | coms.xz_companys_sum,
        | coms.EPC_count,
        | concat(round(coms.EPC_count/coms.xz_companys_sum * 100,2),'%') epc_proportion,
        | coms.company_construction_count,
        | concat(round(coms.company_construction_count/coms.xz_companys_sum * 100,2),'%') construction_proportion,
        | coms.company_build_count,
        | concat(round(coms.company_build_count/coms.xz_companys_sum * 100,2),'%') build_proportion,
        | coms.company_labour_count,
        | concat(round(coms.company_labour_count/coms.xz_companys_sum * 100,2),'%') labour_proportion,
        | coms.xz_local_count,
        | concat(round(coms.xz_local_count/coms.xz_companys_sum * 100,2),'%') xz_local_proportion,
        | coms.xz_nonlocal_count,
        | concat(round(coms.xz_nonlocal_count/coms.xz_companys_sum * 100,2),'%') xz_nonlocal_proportion,
        | coms.xz_other,
        | concat(round(coms.xz_other/coms.xz_companys_sum * 100,2),'%') xz_other_proportion,
        |
        | coms.yw_companys_sum,
        | coms.yw_local_count,
        | concat(round(coms.yw_local_count/coms.yw_companys_sum * 100,2),'%') yw_local_proportion,
        | coms.yw_nonlocal_count,
        | concat(round(coms.yw_nonlocal_count/coms.yw_companys_sum * 100,2),'%') yw_nonlocal_proportion,
        | coms.yw_other,
        | concat(round(coms.yw_other/coms.yw_companys_sum * 100,2),'%') yw_other_proportion
        | from
        |   aj_dict_area area
        | left join (
        |   select
        |     ifnull (xz_com.county_code,yw_com.project_county_code) county_code,
        |     ifnull (xz_com.year,yw_com.year) year,
        |     ifnull (xz_com.month,yw_com.month) month,
        |     ifnull (xz_com.day,yw_com.day) day,
        |     xz_com.xz_companys_sum,
        |     xz_com.EPC_count,
        |	    xz_com.company_construction_count,
        |	    xz_com.company_labour_count,
        |	    xz_com.company_build_count,
        |	    xz_com.xz_local_count,
        |	    xz_com.xz_nonlocal_count,
        |	    xz_com.xz_other,
        |     yw_com.yw_companys_sum,
        |	    yw_com.yw_local_count,
        |	    yw_com.yw_nonlocal_count,
        |	    yw_com.yw_other
        |   from
        |     xz_com full outer join yw_com on xz_com.county_code = yw_com.project_county_code
        |     and xz_com.year = yw_com.year
        |     and xz_com.month = yw_com.month
        |     and xz_com.day = yw_com.day
        |
        | ) coms ON coms.county_code=area.code
        |
        |""".stripMargin


    val dataFrame = spark.sql(sql)


//    dataFrame.show(1000)
    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"ads_companys_day")

    spark.stop()

  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    //获取区域字典
    val aj_dict_area = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","aj_dict_area")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    //温州区域
    aj_dict_area.where("parent_code='330300' and is_deleted=1").createOrReplaceTempView("aj_dict_area")

    //获取业务管辖企业 业务企业(业务本地,业务外地,业务其他) 数量以及占比
    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")

    val cdm_company = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable","cdm_companys")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()
    cdm_company.createOrReplaceTempView("cdm_companys")


  }

}

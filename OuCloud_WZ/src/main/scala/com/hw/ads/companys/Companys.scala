package main.scala.com.hw.ads.companys

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 统计企业 维度指标
 * --数据来源: oucloud_ods----aj_companys
 * 企业类型--company_type:EPC、建设、设计、施工、劳务
 * 企业来源--location:本地 外地
 * 按区域划分:管辖内 行政企业 业务企业(业务本地,业务外地,业务其他) 数量以及占比
 * 行政企业: 依据--county_name
 * 业务企业: 依据--project_county_name
 * @author linzhy
 */
object Companys {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ADS_Companys")
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()
    queryDataCreateView(spark,config)

    //施工、劳务 epc /本地 外地 其他 占比
    val sql=
      """
        |select
        |area.name,
        |coms.companys_sum xz_companys_sum,
        |coms.EPC_count,
        |concat(round(coms.EPC_count/coms.companys_sum * 100,2),'%') epc_proportion,
        |coms.company_construction_count,
        |concat(round(coms.company_construction_count/coms.companys_sum * 100,2),'%') construction_proportion,
        |coms.company_labour_count,
        |concat(round(coms.company_labour_count/coms.companys_sum * 100,2),'%') labour_proportion,
        |coms.company_local_count,
        |concat(round(coms.company_local_count/coms.companys_sum * 100,2),'%') local_proportion,
        |coms.company_nonlocal_count,
        |concat(round(coms.company_nonlocal_count/coms.companys_sum * 100,2),'%') nonlocal_proportion,
        |coms.other,
        |concat(round(coms.other/coms.companys_sum * 100,2),'%') other_proportion,
        |now() create_time
        |from
        |aj_dict_area area
        |left join(
        |	select
        |   com.county_code,
        |	  count(com.cid) companys_sum,
        |	  sum(case when com.company_type=13 then 1 else 0 end) EPC_count,
        |	  sum(case when com.company_type=4 then 1 else 0 end) company_construction_count,
        |	  sum(case when com.company_type=11 then 1 else 0 end) company_labour_count,
        |	  sum(case when com.location=1 then 1 else 0 end) company_local_count,
        |	  sum(case when com.location=2 then 1 else 0 end) company_nonlocal_count,
        |	  sum(case when com.location=2 or com.location=1 then 0 else 1 end) other
        |	from
        |	  aj_companys com group by com.county_code
        |) coms
        |on coms.county_code=area.code
        |
        |""".stripMargin

    val xz_company = spark.sql(sql)



    val ye_sql=
      """
        |select
        | coms.project_county_name name,
        | coms.companys_sum yw_companys_sum,
        | coms.company_local_count yw_company_local_count ,
        | concat(round(coms.company_local_count/coms.companys_sum * 100,2),'%') yw_local_proportion,
        | coms.company_nonlocal_count yw_company_nonlocal_count,
        | concat(round(coms.company_nonlocal_count/coms.companys_sum * 100,2),'%') yw_nonlocal_proportion,
        | coms.other yw_other,
        | concat(round(coms.other/coms.companys_sum * 100,2),'%') yw_other_proportion
        |from (
        | SELECT
        |	  com.project_county_name,
        |	  count(com.cid) companys_sum,
        |	  SUM	( CASE WHEN com.LOCATION = 1 THEN 1 ELSE 0 END ) company_local_count,
        |	  SUM ( CASE WHEN com.LOCATION = 2 THEN 1 ELSE 0 END ) company_nonlocal_count,
        |	  SUM ( CASE WHEN com.LOCATION = 2 OR com.LOCATION = 1 THEN 0 ELSE 1 END ) other
        |FROM
        |	  cdm_companys com
        |where com.project_county_name is not null
        |group by com.project_county_name
        |) coms
        |
        |""".stripMargin

    val yw_company = spark.sql(ye_sql)

    val dataFrame =  xz_company.join(yw_company,Seq("name"),"left")

    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Append,dataFrame,config,"companys_total")

    spark.stop()
  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    //读取comapanys表 信息
    val comdf = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","aj_companys")
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

    //统计指标
    comdf.where("is_deleted=1").createOrReplaceTempView("aj_companys")
    aj_dict_area.where("parent_code='330300' and is_deleted=1").createOrReplaceTempView("aj_dict_area")

    // 获取业务管辖企业 业务企业(业务本地,业务外地,业务其他) 数量以及占比
    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")

    val cdm_company = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable","companys")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()
    cdm_company.where("project_city_name='温州市'").createOrReplaceTempView("cdm_companys")


  }

}

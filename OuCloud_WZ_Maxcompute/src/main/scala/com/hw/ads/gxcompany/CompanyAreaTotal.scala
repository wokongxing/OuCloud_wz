package hw.ads.gxcompany

import hw.utils.SparkUtils

/**
 * 数据来源:
 *    cdm_gx_company_total_detail --企业统计明细中间表
 * 获取字段:
 * `county_code` ----------'区域code',
 * `county_name` ----------'区域名称',
 * `xz_company_total`-------------'行政管辖企业总数量',
 * `xz_company_county_total`------'行政管辖--区域企业数量',
 * `xz_company_proportion`--------'行政管辖区域企业数量占比%',
 * `epc_count`--------------------'epc企业数量',
 * `company_construction_count`---'施工单位的数量',
 * `company_labour_count`---------'劳务单位数量',
 * `company_build_count`----------'设计单位数量',
 * `yw_company_total`-------------'业务管辖总企业数量',
 * `yw_company_county_total`------'业务管辖--区域企业的数量',
 * `yw_company_proportion`--------'业务管辖区域企业的占比',
 * `local_company_total`----------'本地企业数量',
 * `yw_local_proportion`----------'本地企业数量占比%',
 * `nolocal_company_total`--------'外地企业数量,依据county_code 不在温州区域内的',
 * `yw_nolocal_proportion`--------'外地企业数量占比%',
 * `other_company_total`----------'业务管辖区域 其他未知企业数量 ',
 * `yw_other_proportion`----------'其他未知企业数量占比%',
 * `create_time` -----------------'创建时间'
 *
 * 落地:
 *    ads_gx_company_area_total_wz 温州地区--企业区域统计结果表
 *
 * @author linzhy
 */
object CompanyAreaTotal {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |insert overwrite table ads_gx_company_area_total_wz
        |select
        |  coms.code,
        |  coms.name,
        |  coms.xz_company_total,
        |  coms.xz_company_county_total,
        |  concat ( round( coms.xz_company_county_total / coms.xz_company_total * 100, 2 ), '%' ) xz_company_proportion,
        |  coms.epc_count,
        |  coms.company_construction_count,
        |  coms.company_labour_count,
        |  coms.company_build_count,
        |  coms.yw_company_total,
        |  coms.yw_company_county_total,
        |  concat ( round( coms.yw_company_county_total / coms.yw_company_total * 100, 2 ), '%' ) yw_company_proportion,
        |  coms.local_company_total,
        |  concat ( round( coms.local_company_total / coms.yw_company_total * 100, 2 ), '%' ) yw_local_proportion,
        |  coms.nolocal_company_total,
        |  concat ( round( coms.nolocal_company_total / coms.yw_company_total * 100, 2 ), '%' ) yw_nolocal_proportion,
        |  coms.other_company_total,
        |  concat ( round( coms.other_company_total / coms.yw_company_total * 100, 2 ), '%' ) yw_other_proportion,
        |  now() as create_time
        |from (
        |select
        |  area.code,
        |  area.name,
        |  COALESCE(xz1.epc_count,0) epc_count,
        |  COALESCE(xz1.company_construction_count,0) company_construction_count,
        |  COALESCE(xz1.company_labour_count,0) company_labour_count,
        |  COALESCE(xz1.company_build_count,0) company_build_count,
        |  COALESCE(xz.xz_company_total,0) xz_company_county_total,
        |  COALESCE(yw.yw_company_total,0) yw_company_county_total,
        |  COALESCE(yw2.local_company_total,0) local_company_total,
        |  COALESCE(yw3.nolocal_company_total,0) nolocal_company_total,
        |  COALESCE(yw4.other_company_total,0) other_company_total,
        |  sum(xz.xz_company_total) over() xz_company_total,
        |  sum(yw.yw_company_total) over() yw_company_total
        |from
        |(select code,name from ods_gx_dict_area area where area.parentcode=330300) area
        |  left join(
        |    select
        |        county_code,
        |        count(DISTINCT cid) xz_company_total
        |    from
        |        cdm_gx_company_total_detail
        |    where type_top=1
        |    group by county_code
        |  ) xz on area.code = xz.county_code
        |  left join(
        |    select
        |        project_county_code,
        |        count(DISTINCT cid ) yw_company_total
        |    from
        |        cdm_gx_company_total_detail
        |    group by
        |       project_county_code
        |  ) yw on area.code = yw.project_county_code
        |left join (
        |  select
        |    county_code,
        |    sum(case when company_type='1' then 1 else 0 end) epc_count,
        |    sum(case when company_type='2' then 1 else 0 end) company_construction_count,
        |    sum(case when company_type='3' then 1 else 0 end) company_labour_count,
        |    sum(case when company_type='4' then 1 else 0 end) company_build_count
        |  from
        |    cdm_gx_company_total_detail
        |  where type_top=1
        |  group by county_code
        |) xz1 on area.code = xz1.county_code
        |  left join(
        |    select
        |        project_county_code,
        |        count(DISTINCT cid ) local_company_total
        |    from
        |        cdm_gx_company_total_detail
        |    where county_code  in (select code from ods_gx_dict_area where parentcode=330300)
        |    group by
        |       project_county_code
        |  ) yw2 on area.code = yw2.project_county_code
        |  left join(
        |    select
        |        project_county_code,
        |        count(DISTINCT cid ) nolocal_company_total
        |    from
        |        cdm_gx_company_total_detail
        |    where county_code not in (select code from ods_gx_dict_area where parentcode=330300)
        |    and  county_code!=''
        |    group by
        |       project_county_code
        |  ) yw3 on area.code = yw3.project_county_code
        |  left join(
        |    select
        |        project_county_code,
        |        count(DISTINCT cid ) other_company_total
        |    from
        |        cdm_gx_company_total_detail
        |    where  project_county_code  in (select code from ods_gx_dict_area where parentcode=330300)
        |    AND (county_code='' or county_code is null)
        |    group by
        |       project_county_code
        |  ) yw4 on area.code = yw4.project_county_code
        |) coms
        |""".stripMargin

    try{
      import spark._
      sql(sqlstr)
    }finally {
      spark.stop()
    }

  }

}

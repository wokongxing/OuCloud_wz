package hw.ads.gxcompany

import hw.utils.SparkUtils

/**
 * 数据来源:
 *    cdm_gx_company_total_detail --企业统计明细中间表
 * 获取字段:
 *    company_total  --企业总数量;
 *    EPC_count -------EPC企业类型的数量;
 *    epc_proportion -------EPC企业类型的数量占比%;
 *    company_construction_count ----施工企业类型的数量;
 *    construction_proportion ----施工企业类型的数量占比%;
 *    company_labour_count  ---------劳务企业类型的数量;
 *    labour_proportion  ---------劳务企业类型的数量占比%;
 *    company_build_count   ---------设计企业类型的数量;
 *    build_proportion   ---------设计企业类型的数量占比%;
 *    other_company_count   ---------其他未知地区企业数量;
 *    other_company_proportion   ---------其他未知地区企业数量占比%;
 *    local_company_total   ---------本地企业数量;
 *    local_company_proportion   ---------本地企业数量占比%;
 *    nolocal_company_total ---------外地企业数量; 依据county_code 不在温州区域内的
 *    nonlocal_company_proportion ---------外地企业数量占比%;
 *
 * 落地:
 *    ads_gx_company_total_wz 温州地区企业总统计结果表
 *
 * @author linzhy
 */
object CompanyTotal {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |insert overwrite  table ads_gx_company_total_wz
        |select
        |  unix_timestamp() as id,
        |  coms.company_total,
        |  coms.EPC_count,
        |  concat(round(coms.EPC_count/coms.company_total * 100,2),'%') epc_proportion,
        |  coms.company_construction_count,
        |  concat ( round( coms.company_construction_count / coms.company_total * 100, 2 ), '%' ) construction_proportion,
        |  coms.company_labour_count,
        |  concat ( round( coms.company_labour_count / coms.company_total * 100, 2 ), '%' ) labour_proportion,
        |  coms.company_build_count,
        |  concat ( round( coms.company_build_count / coms.company_total * 100, 2 ), '%' ) build_proportion,
        |  coms.other_company_count,
        |  concat ( round( coms.other_company_count / coms.company_total * 100, 2 ), '%' ) other_company_proportion,
        |  coms.local_company_total,
        |  concat ( round( coms.local_company_total / coms.company_total * 100, 2 ), '%' ) local_company_proportion,
        |  coms.nolocal_company_total,
        |  concat ( round( coms.nolocal_company_total / coms.company_total * 100, 2 ), '%' ) nonlocal_company_proportion,
        |  now() create_time
        |from (
        |  select
        |    c1.company_total,
        |    c1.EPC_count,
        |    c1.company_construction_count,
        |    c1.company_labour_count,
        |    c1.company_build_count,
        |    c1.other_company_count,
        |    c2.local_company_total,
        |    c3.nolocal_company_total
        |  from (
        |    select
        |       1 as linkid,
        |       count(DISTINCT cid) company_total,
        |       sum(case when company_type='1' then 1 else 0 end) EPC_count,
        |       sum(case when company_type='2' then 1 else 0 end) company_construction_count,
        |       sum(case when company_type='3' then 1 else 0 end) company_labour_count,
        |       sum(case when company_type='4' then 1 else 0 end) company_build_count,
        |       sum(case when county_code is null or county_code='' then 1 else 0 end) other_company_count
        |    from
        |       cdm_gx_company_total_detail
        |    where type_top=1
        |  ) c1
        |  left join (
        |    select
        |       1 as linkid,
        |       count(DISTINCT cid) local_company_total
        |    from
        |       cdm_gx_company_total_detail
        |    where type_top=1
        |    and county_code in (select code from ods_gx_dict_area  where parentcode=330300 )
        |  ) c2 on c1.linkid=c2.linkid
        |  left join (
        |    select
        |       1 as linkid,
        |       count(DISTINCT cid) nolocal_company_total
        |    from
        |       cdm_gx_company_total_detail
        |    where type_top=1
        |    and county_code not in (select code from ods_gx_dict_area  where parentcode=330300 )
        |    and county_code!=''
        |  ) c3 on c1.linkid=c3.linkid
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

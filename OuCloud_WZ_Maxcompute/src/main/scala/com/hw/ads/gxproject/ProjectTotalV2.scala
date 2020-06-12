package hw.ads.gxproject

import hw.utils.SparkUtils


/**
 * 统计:温州地区 项目信息
 * 数据来源:
 *     cdm_gx_project_day-以天为维度 -项目信息表;
 *     ods_gx_salary_batch--发放薪资批次主表;
 *     ods_gx_labourer--民工表;
 *     ods_gx_company--工薪企业;
 *
 * 获取字段:
 *    '温州' as county_code,
 *    项目总数-project_total;
 *    在建项目数-bulid_total(project_status = 3);
 *    竣工项目数-complete_total(project_status = 2);
 *    停工项目数-stop_total(project_status = 1);
 *    涉及企业数量-companys_sum;
 *    已缴纳保证金企业--is_ompanys_safeguard;
 *    无缴纳保证金企业数--no_ompanys_safeguard
 *    保证金缴纳总额--assure_amt;
 *    已实名制民工人数--realname_total;
 *    已发薪资总金额--real_amt;
 *    劳务费用专项账户开户数--account_total;
 *    维权公示牌数--billboards;
 *
 * 存储位置:
 *    ads_gx_project_total_wz--温州总项目数据统计
 *
 * @author lzhy
 */
object ProjectTotalV2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      """
        |insert overwrite table ads_gx_project_total_wz
        |SELECT
        |   a1.county_code,
        |	  a1.project_total,
        | 	a1.bulid_total,
        | 	a1.complete_total,
        | 	a1.stop_total,
        | 	a1.billboards,
        |   a1.real_amt,
        |  	a2.ompanys_safeguard_total,
        | 	a2.assure_amt,
        | 	a3.realname_total,
        |   a4.companys_total,
        |   a5.account_total,
        |   now() as create_time
        |FROM
        |	(
        |	SELECT  1 as id
        |        ,county_code
        |        ,COUNT(*) project_total
        |        ,sum(CASE WHEN project_condition=1 THEN 1 ELSE 0 END ) AS bulid_total
        |        ,sum(CASE WHEN project_condition=2 THEN 1 ELSE 0 END ) AS stop_total
        |        ,sum(CASE WHEN project_condition=3 THEN 1 ELSE 0 END ) AS complete_total
        |        ,sum(is_billboards) AS billboards
        |        ,sum(real_amt) AS real_amt
        |FROM    cdm_gx_project_detail
        |WHERE   city_code = '330300'
        |AND     is_deleted = 1
        |AND     STATUS IN (3,4,5)
        |GROUP BY county_code
        |	) a1
        |LEFT JOIN (
        |	  	select
        |       1 as id,
        |       count(distinct mcs.cid) ompanys_safeguard_total,
        |       sum(mcs.assure_amt ) assure_amt
        |    from
        |      ods_gx_mig_company_safeguard  mcs
        |    where mcs.is_deleted=1
        |      and (mcs.status=3 or mcs.status=4)
        |	) a2 on a1.id = a2.id
        |LEFT JOIN (
        |   select
        |     1 as id,
        |     count(*) as realname_total
        |   from
        | 	  ods_gx_labourer l
        |   where l.real_grade != 99
        |   and l.is_deleted =1
        | ) a3 on a1.id = a3.id
        |LEFT JOIN (
        |   select
        |     1 as id,
        |     count(*) companys_total
        |   from
        |     ods_gx_company
        |   where is_deleted=1
        |     and status ='3'
        | ) a4 on a1.id = a4.id
        |LEFT JOIN (
        |   select
        |   	1 as id,
        |		  count(*)	as 	account_total
        |	  from
        |		  ods_gx_mig_special_account msa
        |	  where msa.is_deleted = 1
        |	    and msa.status = 1
        | ) a5 on a1.id = a5.id
        |
        |"""

    try{
      import spark._
      sql(sqlstr)
    }finally {
      spark.stop()
    }

  }


}

package hw.ads.companysalary

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * --项目信息总统计
 *  --数据来源:
 *     ads_qx_project;
 *     ads_qx_cache_labourer;
 *     ods_base_mig_company_safeguard;
 *     ods_base_project_billboard;
 *     ods_base_company;
 *
 *
 * @author linzhy
 */
object ProjectTotal extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |insert overwrite table ads_qx_cache_comprehensive_statistics
        |select
        |concat(a.province_code,a.city_code,a.county_code,a.industry_type) as id,
        |count(*) project_count,
        |sum (case when project_condition=3 then 1 else 0 end ) as project_construction,
        |sum (case when project_condition=5 then 1 else 0 end ) as project_stop,
        |sum (case when project_condition=6 then 1 else 0 end ) as project_completed,
        |sum (case when project_condition=4 then 1 else 0 end ) as project_complete,
        |coalesce(sum(a.billboard_count),0) billboard_count,
        |sum(case when amt_top=1 then a.bail_amt_total else 0 end )  bail_amt_total,
        |coalesce(sum(a.account_count),0) account_count,
        |coalesce(sum(a.salary_amt_total),0) salary_amt_total,
        |a.province_code,
        |a.city_code,
        |a.county_code,
        |a.industry_type,
        |'' as create_user,
        |now() as create_time,
        |'' as last_modify_user,
        |now() as last_modify_time,
        |1 as is_deleted
        |from (
        |select
        |  qp.pid,
        |  qp.project_condition,
        |  qp.province_code,
        |  qp.city_code,
        |  qp.county_code,
        |  qp.industry_type,
        |  coalesce(bp.billboard_count,0) billboard_count,
        |  coalesce(cs.assure_amt,0) bail_amt_total,
        |  coalesce(msa.account_status,0) account_count,
        |  coalesce(sb.real_amt,0) salary_amt_total,
        |  1 as is_deleted,
        |  row_number() over (partition by cs.cid,cs.province_code,cs.city_code order by qp.pid desc) amt_top
        |from (
        |  select
        |  pid,
        |  cid,
        |  project_condition,
        |  province_code,
        |  city_code,
        |  county_code,
        |  industry_type
        |  from
        |  ods_base_project
        |  where is_deleted=1 and industry_type!=0
        |) qp
        |  LEFT JOIN (
        |    SELECT
        |    pid,
        |    1 as billboard_count
        |    FROM
        |    ods_base_project_billboard
        |    WHERE is_deleted =1
        |    group by pid
        |  ) bp ON      qp.pid = bp.pid
        |  LEFT JOIN(
        |    SELECT  cid
        |    ,province_code
        |    ,city_code
        |    ,sum(assure_amt) as assure_amt
        |    FROM
        |    ods_base_mig_company_safeguard
        |    WHERE   is_deleted = 1
        |    AND     status in (3,4)
        |    and province_code is not NULL and city_code is not null
        |    and province_code!='' and city_code!=''
        |    group by cid,province_code,city_code
        |  ) cs ON qp.cid = cs.cid
        |  AND     qp.province_code = cs.province_code
        |  AND     qp.city_code = cs.city_code
        |  left join (
        |    select
        |    msa.pid,
        |    1	as 	account_status
        |    from
        |    ods_base_mig_special_account msa
        |    where msa.is_deleted = 1
        |    and msa.status = 1
        |    group by  msa.pid
        |  ) msa on msa.pid = qp.pid
        |  LEFT JOIN (
        |    SELECT
        |    SUM(sb.real_amt) real_amt
        |    ,sb.source_pid
        |    FROM    ods_base_salary_batch sb
        |    WHERE   sb.STATUS = 1
        |    AND     sb.TYPE = 1
        |    GROUP BY source_pid
        |  ) sb ON  qp.pid = sb.source_pid
        |) a
        |group by
        |a.province_code,
        |a.city_code,
        |a.county_code,
        |a.industry_type
        |
        |
        |""".stripMargin

    try{
      import spark._
      sql(sqlstr)

    }finally {
      spark.stop()
    }

  }

}

package hw.ads.companysalary

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --项目民工信息表 聚合表
 *  --数据来源:
 *  ods_base_salary_batch;
 *     ads_base_project;
 *     ods_base_mig_special_account;
 *     ods_base_mig_company_safeguard;
 * @author linzhy
 */
object ProjectCacheMonthStatistics extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |with ac as (
        |  select
        |  CONCAT(p.county_code,year(msa.create_time),month(msa.create_time),p.industry_type) AS id,
        |  p.province_code,
        |  p.city_code,
        |  p.county_code,
        |  p.industry_type,
        |  trunc(msa.create_time,'MM') cur_date,
        |  year(msa.create_time) cur_date_year,
        |  month(msa.create_time) cur_date_month,
        |  count(distinct p.pid) company_safeguard_count
        |  from
        |  ods_base_mig_special_account msa join ods_base_project p
        |    on msa.pid = p.pid and p.is_deleted=1 and p.industry_type!=0
        |  where msa.is_deleted = 1
        |  and msa.status = 1
        |  and year(msa.create_time) = year(now())
        |  and month(msa.create_time) = month(now())
        |  group by p.province_code,p.city_code,p.county_code,p.industry_type,
        |  year(msa.create_time),month(msa.create_time), trunc(msa.create_time,'MM')
        |
        |),
        |cs as (
        |  select
        |    CONCAT(a.county_code,cur_date_year,cur_date_month,industry_type) AS id,
        |    a.province_code,
        |    a.city_code,
        |    a.county_code,
        |    a.industry_type,
        |    a.cur_date,
        |    a.cur_date_year,
        |    a.cur_date_month,
        |    sum(case when amt_top=1 then a.assure_amt else 0 end ) bail_amt
        |from (
        |  SELECT
        |    p.province_code,
        |    p.city_code,
        |    p.county_code,
        |    p.industry_type,
        |    trunc(cs.create_time,'MM') cur_date,
        |    year(cs.create_time) cur_date_year,
        |    month(cs.create_time) cur_date_month,
        |    cs.assure_amt,
        |    row_number() over (partition by cs.cid,cs.province_code,cs.city_code order by p.industry_type desc ) as amt_top
        |  FROM
        |    ods_base_mig_company_safeguard cs join ods_base_project p
        |  ON  p.cid = cs.cid and p.is_deleted=1 and p.industry_type!=0
        |  AND p.province_code = cs.province_code
        |  AND p.city_code = cs.city_code
        |  WHERE    cs.is_deleted = 1
        |  and      year(cs.create_time) = year(now())
        |  and      month(cs.create_time) = month(now())
        |  AND      cs.status in (3,4)
        |  and cs.province_code is not NULL and cs.city_code is not null
        |  and cs.province_code!='' and cs.city_code!=''
        |) a
        |group by
        |    a.province_code,
        |    a.city_code,
        |    a.county_code,
        |    a.industry_type,
        |    a.cur_date,
        |    a.cur_date_year,
        |    a.cur_date_month
        |),
        |sa as (
        |  SELECT
        |  CONCAT(p.county_code,sb.year,sb.month ,p.industry_type) AS id,
        |  p.province_code,
        |  p.city_code,
        |  p.county_code,
        |  p.industry_type,
        |  sb.year_month cur_date,
        |  sb.year cur_date_year,
        |  sb.month  cur_date_month,
        |  SUM(sb.real_amt) salary_amt
        |  FROM    ods_base_salary_batch sb join ods_base_project p
        |    ON  p.pid = sb.source_pid and p.is_deleted=1 and p.industry_type!=0
        |  WHERE   sb.STATUS = 1
        |  AND     sb.TYPE = 1
        |  and sb.year = year(now())
        |  and sb.month = month(now())
        |  GROUP BY p.province_code,p.city_code,p.county_code,p.industry_type
        |  ,sb.year,sb.month,sb.year_month
        |)
        |  select
        |  cast(coalesce(a.id,cs.id) as bigint)  id,
        |  coalesce(a.industry_type,cs.industry_type) industry_type,
        |  coalesce(a.province_code,cs.province_code) province_code,
        |  coalesce(a.city_code,cs.city_code) city_code,
        |  coalesce(a.county_code,cs.county_code) county_code,
        |  coalesce(a.cur_date,cs.cur_date) cur_date,
        |  coalesce(a.cur_date_year,cs.cur_date_year) cur_date_year,
        |  coalesce(a.cur_date_month,cs.cur_date_month) cur_date_month,
        |  coalesce(a.salary_amt,0) salary_amt,
        |  coalesce(a.company_safeguard_count,0) company_safeguard_count,
        |  coalesce(cs.bail_amt,0) bail_amt,
        |  '' as create_user,
        |   now() as create_time,
        |  '' as last_modify_user,
        |   now() as last_modify_time,
        |   1 as is_deleted
        |  from (
        |    select
        |    coalesce(sa.id,ac.id) id,
        |    coalesce(sa.industry_type,ac.industry_type) industry_type,
        |    coalesce(sa.province_code,ac.province_code) province_code,
        |    coalesce(sa.city_code,ac.city_code) city_code,
        |    coalesce(sa.county_code,ac.county_code) county_code,
        |    coalesce(sa.cur_date,ac.cur_date) cur_date,
        |    coalesce(sa.cur_date_year,ac.cur_date_year) cur_date_year,
        |    coalesce(sa.cur_date_month,ac.cur_date_month) cur_date_month,
        |    coalesce(sa.salary_amt,0) salary_amt,
        |    coalesce(ac.company_safeguard_count,0) company_safeguard_count
        |    from
        |    sa full outer join ac on sa.id = ac.id
        |  ) a full outer join cs on a.id = cs.id
        |
        |""".stripMargin

      val sqlstr2  =
        """
          |insert overwrite table ads_qx_cache_month_statistics
          |  select
          |  coalesce(a.id,cs.id) id,
          |  coalesce(a.industry_type,cs.industry_type) industry_type,
          |  coalesce(a.province_code,cs.province_code) province_code,
          |  coalesce(a.city_code,cs.city_code) city_code,
          |  coalesce(a.county_code,cs.county_code) county_code,
          |  coalesce(a.cur_date,cs.cur_date) cur_date,
          |  coalesce(a.cur_date_year,cs.cur_date_year) cur_date_year,
          |  coalesce(a.cur_date_month,cs.cur_date_month) cur_date_month,
          |  coalesce(a.salary_amt,cs.salary_amt) salary_amt,
          |  coalesce(a.company_safeguard_count,cs.company_safeguard_count) company_safeguard_count,
          |  coalesce(a.bail_amt,cs.bail_amt) bail_amt,
          |  '' as create_user,
          |  coalesce(a.create_time,cs.create_time)  as create_time,
          |  '' as last_modify_user,
          |  coalesce(a.last_modify_time,cs.last_modify_time)  as last_modify_time,
          |  1 as is_deleted
          |  from
          |     ads_qx_cache_month_statistics cs
          |  full outer join temp_cache_month a on a.id = cs.id
          |
          |""".stripMargin
    try{
      import spark._
      sql(sqlstr).createOrReplaceTempView("temp_cache_month")
      sql(sqlstr2)
    }finally {
      spark.stop()
    }

  }

}

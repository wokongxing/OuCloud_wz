package hw.ads.labours.dp

import hw.utils.SparkUtils

/**
 * 统计
 *    大屏展示模块统计
 *
 *数据来源: cdm 发薪明细聚合表
 *    ods_gx_project_sub
 *    ods_gx_project
 *    ods_gx_mig_company_safeguard
 *    cdm_gx_salary_detail --发薪明细表
 * 获取字段:
 *    年月 统计数值 保证金企业数量 保证金总金额 发薪项目  发薪企业 发薪人员 发薪总金额
 *
 * @author linzhy
 */
object LabourerSalaryTotal {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      """
        |
        |with pro_all as (
        |  select
        |     P.pid source_pid
        |   ,P.pid
        |   ,P.cid
        |  from
        |   ods_gx_project p
        |  where status in (3,4,5)
        |  and is_deleted=1
        |  and project_type in ('房屋建设工程','附属工程','市政工程','其它')
        |  union
        |  select
        |   ps.source_pid
        |   ,ps.pid
        |   ,ps.unit_cid
        |  from
        |   ods_gx_project_sub	ps
        |  where status in (3,4,5)
        |  and is_deleted=1
        |  and project_type in ('房屋建设工程','附属工程','市政工程','其它')
        |),
        |mcs as (
        |  select
        |     sum (ms.assure_amt) over (order by  ms.year asc,ms.month asc) assure_amt,
        |     ms.year,
        |     ms.month,
        |     2 as type
        |from
        |(
        | SELECT
        |     year(mcs.reg_date) as year
        |    ,month(mcs.reg_date) as month
        |    ,sum( mcs.assure_amt ) assure_amt
        |  FROM    ods_gx_mig_company_safeguard mcs
        |  WHERE   mcs.is_deleted = 1
        |  AND     mcs.STATUS in (3,4)
        |  AND cid in (select cid from pro_all)
        |  group by year(mcs.reg_date),month(mcs.reg_date)
        |) ms
        |),
        |com as (
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),0)) as year
        |      ,month(add_months(now(),0)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and  mcs.reg_date < to_date(add_months(now(),0),'yyyy-MM-dd')
        | union
        |    SELECT
        |       count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-1)) as year
        |      ,month(add_months(now(),-1)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and  mcs.reg_date < to_date(add_months(now(),-1),'yyyy-MM-dd')
        | union
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-2)) as year
        |      ,month(add_months(now(),-2)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and  mcs.reg_date < to_date(add_months(now(),-2),'yyyy-MM-dd')
        | union
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-3)) as year
        |      ,month(add_months(now(),-3)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and  mcs.reg_date < to_date(add_months(now(),-3),'yyyy-MM-dd')
        | union
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-4)) as year
        |      ,month(add_months(now(),-4)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and  mcs.reg_date < to_date(add_months(now(),-4),'yyyy-MM-dd')
        | union
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-5)) as year
        |      ,month(add_months(now(),-5)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and    mcs.reg_date < to_date(add_months(now(),-5),'yyyy-MM-dd')
        | union
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-6)) as year
        |      ,month(add_months(now(),-6)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and    mcs.reg_date < to_date(add_months(now(),-6),'yyyy-MM-dd')
        | union
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-7)) as year
        |      ,month(add_months(now(),-7)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and    mcs.reg_date < to_date(add_months(now(),-7),'yyyy-MM-dd')
        | union
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-8)) as year
        |      ,month(add_months(now(),-8)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and     mcs.reg_date < to_date(add_months(now(),-8),'yyyy-MM-dd')
        | union
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-9)) as year
        |      ,month(add_months(now(),-9)) as month
        |    FROM    ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and    mcs.reg_date < to_date(add_months(now(),-9),'yyyy-MM-dd')
        | union
        |    SELECT
        |     count(distinct mcs.cid) company_sum
        |     ,year(add_months(now(),-10)) as year
        |     ,month(add_months(now(),-10)) as month
        |    FROM
        |      ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and    mcs.reg_date < to_date(add_months(now(),-10),'yyyy-MM-dd')
        | union
        |    SELECT
        |      count(distinct mcs.cid) company_sum
        |      ,year(add_months(now(),-11)) as year
        |      ,month(add_months(now(),-11)) as month
        |    FROM
        |      ods_gx_mig_company_safeguard mcs join pro_all on  pro_all.cid = mcs.cid
        |    WHERE   mcs.is_deleted = 1
        |    AND     mcs.STATUS in (3,4)
        |    and    mcs.reg_date < to_date(add_months(now(),-11),'yyyy-MM-dd')
        |),
        |salary as (
        |  select
        |   sum(sa.real_amt) real_amt,
        |   count(DISTINCT sa.salary_cid) company_salary_sum,
        |   count(DISTINCT sa.source_pid) project_count,
        |   count(DISTINCT sa.id_card_no) labourer_sum,
        |   year(add_months(now(),0)) as year,
        |   month(add_months(now(),0)) as month
        |  from
        |   cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),0),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-1)) as year,
        |  month(add_months(now(),-1)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-1),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-2)) as year,
        |  month(add_months(now(),-2)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-2),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-3)) as year,
        |  month(add_months(now(),-3)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-3),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-4)) as year,
        |  month(add_months(now(),-4)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-4),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-5)) as year,
        |  month(add_months(now(),-5)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-5),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-6)) as year,
        |  month(add_months(now(),-6)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-6),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-7)) as year,
        |  month(add_months(now(),-7)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-7),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-8)) as year,
        |  month(add_months(now(),-8)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-8),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-9)) as year,
        |  month(add_months(now(),-9)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-9),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-10)) as year,
        |  month(add_months(now(),-10)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-10),'yyyy-MM-dd')
        |  union
        |  select
        |  sum(sa.real_amt) real_amt,
        |  count(DISTINCT sa.salary_cid) company_salary_sum,
        |  count(DISTINCT sa.source_pid) project_count,
        |  count(DISTINCT sa.id_card_no) labourer_sum,
        |  year(add_months(now(),-11)) as year,
        |  month(add_months(now(),-11)) as month
        |  from
        |  cdm_gx_salary_detail sa
        |  where sa.status=1 and sa.year_month < to_date(add_months(now(),-11),'yyyy-MM-dd')
        |)
        |insert overwrite table ads_dp_labourer_salary_total_month_p partition (date_year,date_month)
        |select
        |  year,
        |  month,
        |  cast(company_sum as long) company_sum,
        |  type,
        |  year date_year,
        |  month date_month
        |from (
        |select
        |  year,
        |  month,
        |  company_sum,
        |  1 as type
        |from com
        |union
        |select
        |  year,
        |  month,
        |  assure_amt,
        |  2 as type
        |from mcs
        |union
        |select
        |  year,
        |  month,
        |  company_salary_sum,
        |  3 as type
        |from
        |salary
        |union
        |select
        |  year,
        |  month,
        |  project_count,
        |  4 as type
        |from
        |salary
        |union
        |select
        |  year,
        |  month,
        |  labourer_sum,
        |  5 as type
        |from
        |salary
        |union
        |select
        |  year,
        |  month,
        |  cast (real_amt/10000 as long) as real_amt,
        |  6 as type
        |from
        |salary
        |) a
        |
        |""".stripMargin

    val sql2 =
      """
        |insert overwrite table temp_dp_labourer_salary_total_month
        |select
        | year,
        | month,
        | amount,
        | type,
        | concat(year,if(length(month)=1,concat(0,month),month)) year_month
        |from
        | ads_dp_labourer_salary_total_month_p
        |where date_year=2020 or date_year=2019 or date_year=2021 or date_year=2022
        |
        |""".stripMargin
    try {
      import spark._
      sql(sqlstr)
      sql(sql2)
    }finally {
      spark.stop()
    }

  }


}

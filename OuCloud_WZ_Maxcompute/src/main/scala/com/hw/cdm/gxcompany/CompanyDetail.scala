package hw.cdm.gxcompany

import hw.utils.SparkUtils

/**
 * 数据来源:
 *    ods_gx_company ------企业;
 *    ods_gx_project ------源项目;
 *    ods_gx_project_sub---子项目;
 *    ods_gx_salary_batch --发薪批次主表;
 *    ods_gx_mig_company_safeguard ----企业保障金表;
 *    ods_gx_mig_special_account ----专项账户备案信息表;
 *  获取字段:
 * `cid`-----------------'cid',
 * `company_name`--------'企业名称',
 * `company_type`--------'企业类型',
 * `county_code`---------'区域code',
 * `type`----------------'保证金缴纳形式',
 * `safeguard_status`----'保证金状态 1-已缴纳 0-未缴纳',
 * `labour_proportion`---'劳务单位的数量比%',
 * `last_month_amt`------'上一个月发薪金额',
 * `this_month_amt`------'本月发薪金额',
 * `year_month`----------'最近发薪年月',
 * `account_status`------'专项账户备案状态,1--已备案,0--未备案',
 * `is_salary`-----------'本月薪资是否发放',
 * `project_count`-------'企业负责的项目数',
 * `create_time`---------'创建时间'
 * 落地:
 *  ads_gx_company_detail
 *
 * @author linzhy
 */
object CompanyDetail {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |with sb as (
        |  SELECT
        |    sb.cid,
        |    sb.year,
        |    sb.month,
        |    sum( amt ) amt,
        |    row_number() over (partition by cid order by sb.year desc,sb.month desc) salary_top
        |  FROM
        |    ods_gx_salary_batch sb
        |  WHERE sb.STATUS = 1
        |  and sb.is_deleted=1
        |  group by
        |    cid,
        |    sb.year,
        |    sb.month
        |)
        |select
        |  com.cid,
        |  com.company_name,
        |  com.company_type,
        |  com.county_code,
        |  mcs.type,
        |  ifnull(mcs.status,1) as safeguard_status,
        |  coalesce(sb1.amt,0) as last_month_amt,
        |  coalesce(sb2.amt,0) as this_month_amt,
        |  case when sb3.year is null and sb3.month is null then '--' else concat(sb3.year ,'年',sb3.month,'月') end as year_month,
        |  coalesce(msa.account_status,0)  account_status,
        |  case when sb2.amt is not null and sb2.amt > 0 then 1 else 0 end as is_salary,
        |  ifnull(pro.project_count,0) project_count,
        |  now() create_time
        |from (
        |  SELECT
        |    cid,
        |    company_name,
        |    county_code,
        |    company_type
        |  FROM
        |    ods_gx_company com
        |  WHERE is_deleted = 1
        |  AND STATUS = 3
        |) com
        |left join ods_gx_mig_company_safeguard mcs on com.cid = mcs.cid
        |  and mcs.is_deleted=1
        |  and mcs.status in (3,4)
        |left join  sb sb1 on com.cid = sb1.cid
        |  and sb1.year= year(add_months(now(),-1))
        |  and sb1.month = month(add_months(now(),-1))
        |left join  sb sb2 on com.cid = sb2.cid
        |  and sb2.year= year(now())
        |  and sb2.month = month(now())
        |left join  sb sb3 on com.cid = sb3.cid
        |  and sb3.salary_top=1
        |left join (
        |  SELECT
        |    msa.cid,
        |    1 as account_status
        |  FROM
        |    ods_gx_mig_special_account msa
        |  WHERE msa.STATUS = 1
        |  and msa.is_deleted=1
        |  group by cid
        |) msa on com.cid = msa.cid
        |left join (
        |   select
        |      cid,
        |      count(*) as project_count
        |    from(
        |      select
        |        cid,
        |        pid
        |      from
        |      ods_gx_project
        |      where is_deleted=1
        |      union
        |      select
        |        unit_cid cid,
        |        pid
        |      from
        |        ods_gx_project_sub
        |      where is_deleted=1
        |  ) p group by cid
        |) pro on com.cid = pro.cid
        |
        |""".stripMargin

    val sqlstr2=
      """
        |insert overwrite table ads_gx_company_detail
        |select
        |  cid,
        |  company_name,
        |  company_type,
        |  county_code,
        |  type,
        |  safeguard_status,
        |  last_month_amt,
        |  this_month_amt,
        |  year_month,
        |  account_status,
        |  is_salary,
        |  project_count,
        |  create_time
        |from
        |   company_detail_view
        |""".stripMargin

    try{
      import spark._
      sql(sqlstr).createOrReplaceTempView("company_detail_view")
      sql(sqlstr2)
    }finally {
      spark.stop()
    }

  }

}

package hw.ads.companysalary

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * --六项总统计
 *  --数据来源:
 *    ods_base_company;
 *    ods_base_project;
 *    ods_base_company_labourer;
 *    ods_base_mig_company_safeguard;
 *    ods_base_mig_special_account;
 *    ods_base_project_tripartite_agreement;
 *    ods_base_bank_transfer
 *
 *  --获取字段: project_norm_fulfil_new
 * `id` bigint(20) NOT NULL AUTO_INCREMENT,
 * `pid` bigint(20) NOT NULL COMMENT '项目id',
 * `project_name` varchar(200) NOT NULL COMMENT '项目名称',
 * `p_status` int(11) NOT NULL COMMENT '项目状态 1：正常，2：停工，3：竣工',
 * `cid` bigint(20) NOT NULL COMMENT '企业id',
 * `company_name` varchar(200) NOT NULL COMMENT '企业名称',
 * `investment_nature` int(11) NOT NULL COMMENT '投资性质(1，非政府投资 2，政府投资 )',
 * `province_code` varchar(20) NOT NULL COMMENT '所属省编号',
 * `city_code` varchar(20) NOT NULL COMMENT '所属市编号',
 * `county_code` varchar(20) NOT NULL COMMENT '所属区编号',
 * `construct_cost` decimal(16,4) NOT NULL COMMENT '工程造价',
 *
 * `real_status` int(11) NOT NULL COMMENT '实名制落实状态 1，未落实  2，已处理  3，已落实',
 * `real_book_status` int(11) NOT NULL COMMENT '实名制是否落实-花名册 1，未落实  2，已处理  3，已落实',
// * `real_contract_status` int(11) NOT NULL COMMENT '实名制是否落实-劳动合同 1，未落实  2，已处理  3，已落实',
 * `real_pay_status` int(11) NOT NULL COMMENT '实名制是否落实-工资表 1，未落实  2，已处理  3，已落实',
 *
 * `bail_status` int(11) NOT NULL COMMENT '工资支付保证金落实状态 1，未落实  2，已处理  3，已落实',
 *
 * `account_status` int(11) NOT NULL COMMENT '分账管理落实状态1，未落实  2，已处理  3，已落实',
 * `account_special_status` int(11) NOT NULL COMMENT '分账管理是够落实-劳务费专户1，未落实  2，已处理  3，已落实',
 * `account_in_status` int(11) NOT NULL COMMENT '分账管理是够落实-进账记录 1，未落实  2，已处理  3，已落实',
 * `account_three_status` int(11) NOT NULL COMMENT '分账管理是够落实-三方协议1，未落实  2，已处理  3，已落实',
 *
 * `proxy_pay_status` int(11) NOT NULL COMMENT '银行代发工资落实状态1，未落实  2，已处理  3，已落实',
 * `proxy_pay_payroll_status` int(11) NOT NULL COMMENT '银行代发工资是否落实-工资表1，未落实  2，已处理  3，已落实', ads_qx_project_info_proxy_pay
 * `proxy_pay_entrustl_status` int(11) NOT NULL COMMENT '银行代发工资是否落实-代发工资委托书1，未落实  2，已处理  3，已落实', 不作为六项统计规则
 * `proxy_pay_islw` bit(1) NOT NULL COMMENT '银行代发工资是否落实-是否有劳务公司(true 有，false 没有)',
 *
 * `billboard_status` int(11) NOT NULL COMMENT '维权告示牌落实状态1，未落实  2，已处理  3，已落实',
 *
 * `pay_status` int(11) NOT NULL COMMENT '按月足额支付落实状态1，未落实  2，已处理  3，已落实', temp_project_info_pay
 *
 * `create_user` varchar(200) NOT NULL COMMENT '创建人',
 * `create_time` datetime NOT NULL COMMENT '创建时间',
 * `last_modify_user` varchar(200) NOT NULL COMMENT '修改人',
 * `last_modify_time` datetime NOT NULL COMMENT '修改时间',
 * `is_deleted` int(11) NOT NULL DEFAULT '1' COMMENT '是否删除 1:有效 2：删除',
 *
 * @author linzhy
 */
object ProjectNormFulfilNewTotal extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    //获取需要统计的时间
    val datetime = spark.conf.get("spark.project.datetime")
    val sqlstr =
      s"""
         |SELECT
         |         qp.pid id
         |        ,qp.pid
         |        ,qp.industry_type
         |        ,qp.project_type
         |        ,qp.project_name
         |        ,qp.project_condition p_status
         |        ,qp.cid
         |        ,ifnull(qc.company_name,'--') company_name
         |        ,qp.investment_nature
         |        ,qp.province_code
         |        ,qp.city_code
         |        ,qp.county_code
         |        ,qp.construct_cost
         |        ,1 as real_status
         |        ,coalesce(cl.real_book_status,1) real_book_status
         |        ,3 AS real_contract_status
         |        ,case when ( day(now()) - day(date_add(trunc(now(),'MM'),qp.pay_day+4)) ) > 0 and month(now()) = month(date_add(trunc(now(),'MM'),qp.pay_day+4))
         |              then coalesce(SB1.real_pay_status,1)
         |              else coalesce(SB2.real_pay_status,1)
         |          end AS real_pay_status
         |        ,coalesce(cs.bail_status,1) AS bail_status
         |        ,CASE    WHEN sa.account_special_status=3  AND ta.account_three_status=3 THEN 3
         |                 ELSE 1
         |         END account_status
         |        ,coalesce(sa.account_special_status,1) AS account_special_status
         |        ,case when ( day(now()) - day(date_add(trunc(now(),'MM'),qp.pay_day+4)) ) > 0
         |              and month(now()) = month(date_add(trunc(now(),'MM'),qp.pay_day+4))
         |              then coalesce(bt.account_in_status,1)
         |          else coalesce(bt2.account_in_status,1)
         |         end AS account_in_status
         |        ,coalesce(ta.account_three_status,1) AS account_three_status
         |        , 3 as proxy_pay_status
         |        ,case when ( day(now()) - day(date_add(trunc(now(),'MM'),qp.pay_day+4)) ) > 0 and month(now()) = month(date_add(trunc(now(),'MM'),qp.pay_day+4))
         |              then coalesce(SB1.real_pay_status,1)
         |              else coalesce(SB2.real_pay_status,1)
         |          end AS proxy_pay_payroll_status
         |        ,case
         |          when qp.city_code='330300' and qp.industry_type=1 and coalesce(ps.status,0)=1
         |              and coalesce(pc2.wtb_count,0)=0 then 1
         |          when qp.city_code='330300' and qp.industry_type=1 and coalesce(ps.status,0)=1
         |              and coalesce(pc2.wtb_count,0)>0 and coalesce(pc2.wtb_count,0)< coalesce(pc1.wta_count,0) then 4
         |          when qp.city_code='330300' and qp.industry_type=1 and coalesce(ps.status,0)=1
         |              and coalesce(pc2.wtb_count,0)>0 and coalesce(pc2.wtb_count,0)=coalesce(pc1.wta_count,0)  then 3
         |          when qp.city_code='330300' and qp.industry_type=1 and coalesce(ps.status,0)=0  then 1
         |          when coalesce(pc2.wtb_count,0)>0 then 3
         |          else 1 end AS proxy_pay_entrustl_status
         |        ,case when qp.city_code='330300' and qp.industry_type=1 then coalesce(ps.status,0)
         |              when coalesce(pc2.wtb_count,0)>0 then 1
         |              else 0
         |         end AS proxy_pay_islw
         |        ,coalesce(bp.status,1) AS billboard_status
         |        ,case when ( day(now()) - day(date_add(trunc(now(),'MM'),qp.pay_day+4)) ) > 0 and month(now()) = month(date_add(trunc(now(),'MM'),qp.pay_day+4))
         |              then coalesce(SB1.real_pay_status,1)
         |              else coalesce(SB2.real_pay_status,1)
         |          end AS pay_status
         |        ,'' AS create_user
         |        ,now() AS create_time
         |        ,'' AS last_modify_user
         |        ,now() AS last_modify_time
         |        ,qp.is_deleted
         |        ,coalesce(pc1.wta_count,0) AS wta_count
         |        ,coalesce(pc2.wtb_count,0) AS wtb_count
         |        ,year(now()) date_year
         |        ,month(now()) date_month
         |FROM ods_base_project  qp
         |LEFT JOIN (
         |   select
         |      cid,
         |      company_name
         |   from
         |      ods_base_company
         |   group by cid, company_name
         |) qc
         |ON      qp.cid = qc.cid
         |LEFT JOIN(
         |            SELECT  source_pid
         |                    ,3 AS real_book_status
         |            FROM    ods_base_labourer_resume
         |            WHERE   is_job = true
         |            AND     is_deleted = 1
         |            GROUP BY source_pid
         |        ) cl
         |ON      qp.pid = cl.source_pid
         |LEFT JOIN(
         |           SELECT  cid
         |              ,province_code
         |              ,city_code
         |              ,3 AS bail_status
         |            FROM
         |              ods_base_mig_company_safeguard
         |          WHERE   is_deleted = 1
         |          AND     status in (3,4)
         |          and province_code is not NULL and city_code is not null
         |          and province_code!='' and city_code!=''
         |          group by cid,province_code,city_code
         |        ) cs
         |ON      qp.cid = cs.cid
         |AND     qp.province_code = cs.province_code
         |AND     qp.city_code = cs.city_code
         |LEFT JOIN (
         |            SELECT
         |	            pid,
         |	            3 as account_special_status
         |            FROM
         |	            ods_base_mig_special_account
         |            WHERE is_deleted = 1
         |            AND status =1
         |            group by pid
         |          ) sa
         |ON      qp.pid = sa.pid
         |LEFT JOIN(
         |            SELECT  pid
         |                    ,CASE    WHEN SUM(amt)>0 THEN 3
         |                             ELSE 1
         |                     END account_in_status
         |            FROM    ods_base_bank_transfer
         |            WHERE   jtype = 1
         |            AND     is_deleted = 1
         |            and     is_pro_valid>0
         |            AND     year(trans_time) = year(add_months(now(),-1))
         |            AND     month(trans_time) = month(add_months(now(),-1))
         |            GROUP BY pid
         |        ) bt
         |ON      qp.pid = bt.pid
         |LEFT JOIN(
         |            SELECT  pid
         |                    ,CASE    WHEN SUM(amt)>0 THEN 3
         |                             ELSE 1
         |                     END account_in_status
         |            FROM    ods_base_bank_transfer
         |            WHERE   jtype = 1
         |            AND     is_deleted = 1
         |            and     is_pro_valid>0
         |            AND     year(trans_time) = year(add_months(now(),-2))
         |            AND     month(trans_time) = month(add_months(now(),-2))
         |            GROUP BY pid
         |        ) bt2
         |ON      qp.pid = bt2.pid
         |LEFT JOIN (
         |              SELECT  ta.pid
         |                      ,3 AS account_three_status
         |              FROM    ods_base_project_tripartite_agreement ta join ods_base_company com
         |              on ta.sg_cid = com.cid and com.is_deleted=1
         |              WHERE   ta.is_deleted = 1
         |              GROUP BY ta.pid
         |          ) ta
         |ON      qp.pid = ta.pid
         |LEFT JOIN (
         |    SELECT
         |	    pid,
         |	    3 as status
         |   FROM
         |	    ods_base_project_billboard
         |   group by pid
         |) bp
         |ON      qp.pid = bp.pid
         |left join (
         |  select
         |    ps.source_pid,
         |    1 as status
         |  from
         |    ods_gx_project_sub ps join ods_gx_company c on ps.unit_cid=c.cid
         |  where ps.is_deleted=1 and ps.status in (3,4)  and c.is_deleted=1
         |  group by ps.source_pid
         |) ps on qp.pid = ps.source_pid
         |left join (
         |  select
         |    ps.source_pid,
         |    count(DISTINCT c.organization_code) wta_count
         |  from ods_gx_project_sub ps
         |  join ods_gx_company c on ps.unit_cid=c.cid
         |  where ps.is_deleted=1
         |  and ps.status in (3,4)
         |  and c.is_deleted=1
         |  group by ps.source_pid
         |) pc1 on qp.pid = pc1.source_pid
         |left join (
         |  select
         |    pid,
         |    count(DISTINCT subcontract_company_organization_code) wtb_count
         |  from
         |    ods_base_project_commission_attachment
         |  where is_deleted=1 group by pid
         |) pc2 on qp.pid = pc2.pid
         |left join (
         |   SELECT
         |	    sb.source_pid,
         |	    3 as real_pay_status
         |  FROM
         |   ods_base_salary_batch sb
         |  WHERE sb.is_deleted = 1
         |  AND sb.status = 1
         |	 AND sb.YEAR =  year(add_months(now(),-1))
         |	 AND sb.MONTH = month(add_months(now(),-1))
         |  GROUP BY sb.source_pid
         |) SB1 on qp.pid = SB1.source_pid
         |left join (
         |   SELECT
         |	    sb.source_pid,
         |	    3 as real_pay_status
         |  FROM
         |   ods_base_salary_batch sb
         |  WHERE sb.is_deleted = 1
         |  AND sb.status = 1
         |	 AND sb.YEAR =  year(add_months(now(),-2))
         |	 AND sb.MONTH =  month(add_months(now(),-2))
         |  GROUP BY sb.source_pid
         |) SB2 on qp.pid = SB2.source_pid
         |""".stripMargin

    val sqlstr2=
      """
        |insert overwrite table ads_qx_project_norm_fulfil_new_p partition (date_year,date_month)
        |SELECT
        |        id
        |        ,pid
        |        ,industry_type
        |        ,project_type
        |        ,project_name
        |        ,p_status
        |        ,cid
        |        ,company_name
        |        ,investment_nature
        |        ,province_code
        |        ,city_code
        |        ,county_code
        |        ,construct_cost
        |        ,CASE    WHEN real_book_status=3 AND real_pay_status=3 THEN 3
        |                 ELSE 1
        |         END as real_status
        |        ,real_book_status
        |        ,real_contract_status
        |        ,real_pay_status
        |        ,bail_status
        |        ,account_status
        |        ,account_special_status
        |        ,account_in_status
        |        ,account_three_status
        |        ,CASE
        |           WHEN proxy_pay_payroll_status=3  and proxy_pay_islw=1 and proxy_pay_entrustl_status=3 THEN 3
        |           WHEN proxy_pay_payroll_status=3  and proxy_pay_islw=0 and proxy_pay_entrustl_status=1 THEN 3
        |           ELSE 1
        |         END proxy_pay_status
        |        ,proxy_pay_payroll_status
        |        ,proxy_pay_entrustl_status
        |        ,proxy_pay_islw
        |        ,billboard_status
        |        ,pay_status
        |        ,create_user
        |        ,create_time
        |        ,last_modify_user
        |        ,last_modify_time
        |        ,is_deleted
        |        ,date_year
        |        ,date_month
        |FROM    temp_view
        |""".stripMargin

    val sqlstr3=
      """
        |insert overwrite table ads_qx_project_norm_fulfil_new
        |SELECT
        |        id
        |        ,pid
        |        ,industry_type
        |        ,project_type
        |        ,project_name
        |        ,p_status
        |        ,cid
        |        ,company_name
        |        ,investment_nature
        |        ,province_code
        |        ,city_code
        |        ,county_code
        |        ,construct_cost
        |        ,CASE    WHEN real_book_status=3 AND real_pay_status=3 THEN 3
        |                 ELSE 1
        |         END as real_status
        |        ,real_book_status
        |        ,real_contract_status
        |        ,real_pay_status
        |        ,bail_status
        |        ,account_status
        |        ,account_special_status
        |        ,account_in_status
        |        ,account_three_status
        |        ,CASE
        |           WHEN proxy_pay_payroll_status=3  and proxy_pay_islw=1 and proxy_pay_entrustl_status=3 THEN 3
        |           WHEN proxy_pay_payroll_status=3  and proxy_pay_islw=0 and proxy_pay_entrustl_status=1 THEN 3
        |           ELSE 1
        |         END proxy_pay_status
        |        ,proxy_pay_payroll_status
        |        ,proxy_pay_entrustl_status
        |        ,proxy_pay_islw
        |        ,billboard_status
        |        ,pay_status
        |        ,create_user
        |        ,create_time
        |        ,last_modify_user
        |        ,last_modify_time
        |        ,is_deleted
        |FROM    temp_view
        |""".stripMargin

    try{
      import  spark._
      val frame = sql(sqlstr)
      frame.createOrReplaceTempView("temp_view")
      sql(sqlstr2)
      sql(sqlstr3)
    }finally {
      spark.stop()
    }

  }

}

package hw.cdm.gxproject

import hw.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/**项目维度指标 数据聚合 清洗 -每天凌晨运行
 * 数据来源:
 *    ods_gx_project-项目信息表;
 *    ods_gx_project_sub-项目信息子表;
 *    ods_gx_company_labourer--企业员工关系表,
 *    ods_gx_mig_special_account--专项账户表,
 *    ods_gx_bank_safeguard--企业保障金表;
 *    ods_gx_project_billboard--项目公示牌;
 *    ods_gx_project_attendance_ext-项目考勤表;
 *    ods_gx_audit--审批表;
 *    ods_gx_project_attachment--项目劳务合同附件表;
 *    ods_gx_project_tripartite_agreement--三方监管协议;
 *    ods_gx_salary_batch -- 发薪批次主表;
 *    ods_gx_salary_sub -- 发薪批次明细表;
 *    ods_gx_account_transfer --转账批次表;
 * 获取字段:
 *    项目id,企业id,项目类型-(1-房屋;2-市政;3-附属;99-其他),
 *    company_name 企业名称;
 *    company_type 企业类型：1：建设集团，2：施工单位，3：劳务企业，4：设计单位
 *    status 项目状态 1：已保存（审核未通过） 2：提交（未审核） 3：待分配（审核通过）4：进行中（已分配）5：完结,
 *    is_deleted--是否被删除 1 --否; 2--是,
 *    response_code -- ------项目代码/责任编码,
 *    principal_empid -- ------责任人,
 * 	  build_area -- ----------项目建设面积,
 *    construct_cost --------项目投资总额,
 *    create_user ----------项目创建人,
 *    project_condition----项目状态(1-正常,2-停工,3-竣工),
 *    项目省市区,项目创建时间(年月日),
 *    is_realname---------项目已实名(1-是,0-否),
 *    labourers_count----------项目实名员工人数,
 *    project_account_status---是否有专项账户(1-是,0-否),
 *    is_bank_account---是否已银行卡发放工资(1-是,0-否),
 *    safeguard_status---------已缴纳保障金(1-是,0-否)
 *    assure_amt --------------保证金金额(单位万)
 *    is_billboards------------项目公示牌数,
 * 	  is_attendance------------项目是否云考勤(1-是,0-否),
 *    contract_status----------项目是否签订劳务合同(1-是,0-否),
 * 	  is_agreement-------------项目是否有三方监管协议(1-是,0-否)
 *    real_amt -- -------------项目所发薪资;
 *    this_month_amt ----------本月所发薪资
 *  存储位置:
 *    cdm_gx_project_detail
 * @author lzhy
 */
object ProjectDetail {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)
    val sqlstr=
      """
        |
        |with pro_all as (
        |  select
        |			 P.pid source_pid
        |			,P.pid
        |			,P.cid
        |		from
        |			ods_gx_project p
        |  where status in (3,4,5)
        |  and is_deleted=1
        |  and project_condition=1
        |  and project_type in ('房屋建设工程','附属工程','市政工程','其它')
        |union
        |		select
        |			 ps.source_pid
        |			,ps.pid
        |			,ps.unit_cid cid
        |		from
        |			ods_gx_project_sub	ps
        |  where status in (3,4)
        |  and is_deleted=1
        |  and project_condition=1
        |  and project_type in ('房屋建设工程','附属工程','市政工程','其它')
        |),
        | coms as (
        |   SELECT
        |  pro.source_pid,
        |  com.company_type,
        |  collect_set(com.company_name) as company_names,
        |  concat_ws("&",collect_set(
        |       case when com.is_pay=1 then com.company_safeguard_detail else null end
        |  ) ) company_safeguard_detail,
        |  concat_ws("&",collect_set(
        |       case when com.is_pay=0 then com.company_safeguard_detail else null end
        |  ) ) nopay_company_safeguard_detail,
        |  concat_ws("&",collect_set(
        |       case when com.is_account=1 then com.company_account_detail else null end
        |  ) ) company_account_detail,
        |  concat_ws("&",collect_set(
        |       case when com.is_account=0 then com.company_account_detail else null end
        |  ) ) no_company_account_detail
        |  FROM
        |  (select source_pid,cid from pro_all group by source_pid,cid) pro
        |    JOIN (
        |      select
        |        com.cid,
        |        com.company_type,
        |        com.company_name,
        |        concat('[',
        |        case when cs.status=3 then '在缴'
        |             when cs.status=4 then '免缴'
        |             else '未缴'
        |             end
        |        ,']',com.company_name,'(',coalesce(cs.assure_amt,0),'万元)') company_safeguard_detail,
        |        case when cs.status=3 or cs.status=4 then 1
        |             else 0
        |             end is_pay,
        |        concat('[',
        |        case when msa.status=1 then '已备案'
        |             else '未备案'
        |        end
        |                ,']',com.company_name) company_account_detail,
        |        case when msa.status=1 then 1
        |        else 0
        |        end is_account
        |      from
        |        ods_gx_company com
        |      left join ods_gx_mig_company_safeguard cs
        |           on com.cid = cs.cid
        |       and cs.is_deleted = 1
        |      left join ods_gx_mig_special_account  msa
        |           on  com.cid = msa.cid
        |       and msa.is_deleted = 1
        |    ) com ON pro.cid = com.cid
        |  group by pro.source_pid,com.company_type
        | ),
        | AM AS(
        |   SELECT
        |         pro_all.source_pid
        |        ,CAST(sum(amt) as DECIMAL(15,2))  amt
        |   FROM    pro_all
        |   JOIN    (
        |            SELECT  pid
        |                    ,sum(amt) amt
        |            FROM    ods_gx_account_transfer at
        |            WHERE   is_deleted = 1
        |            AND     STATUS = 1
        |            GROUP BY pid
        |            UNION ALL
        |            SELECT  pid
        |                    ,sum(real_amt) amt
        |            FROM    ods_gx_salary_sub at
        |            WHERE   is_deleted = 1
        |            AND     STATUS = 1
        |            GROUP BY pid
        |        ) AM
        |   ON      pro_all.pid = AM.pid
        |   GROUP BY pro_all.source_pid
        | )
        |SELECT
        |         P.pid
        |        ,P.cid
        |        ,gc.company_name
        |        ,gc.company_type
        |        ,P.project_name
        |        ,P.response_code
        |        ,COM1.company_names epc_company_names
        |        ,COM1.company_safeguard_detail epc_company_safeguard_detail
        |        ,COM1.nopay_company_safeguard_detail epc_nopay_company_safeguard_detail
        |        ,COM1.company_account_detail epc_company_account_detail
        |        ,COM1.no_company_account_detail epc_no_company_account_detail
        |        ,COM2.company_names construction_company_names
        |        ,COM2.company_safeguard_detail construction_company_safeguard_detail
        |        ,COM2.nopay_company_safeguard_detail construction_nopay_company_safeguard_detail
        |        ,COM2.company_account_detail construction_company_account_detail
        |        ,COM2.no_company_account_detail construction_no_company_account_detail
        |        ,COM3.company_names labour_company_names
        |        ,COM3.company_safeguard_detail labour_company_safeguard_detail
        |        ,COM3.nopay_company_safeguard_detail labour_nopay_company_safeguard_detail
        |        ,COM3.company_account_detail labour_company_account_detail
        |        ,COM3.no_company_account_detail labour_no_company_account_detail
        |        ,COM4.company_names design_company_names
        |        ,COM4.company_safeguard_detail design_company_safeguard_detail
        |        ,COM4.nopay_company_safeguard_detail design_nopay_company_safeguard_detail
        |        ,COM4.company_account_detail design_company_account_detail
        |        ,COM4.no_company_account_detail design_no_company_account_detail
        |        ,P.principal_empid
        |        ,P.build_area
        |        ,P.construct_cost
        |        ,P.project_type
        |        ,P.status
        |        ,P.project_condition
        |        ,P.create_user
        |        ,P.is_deleted
        |        ,P.province_code
        |        ,P.city_code
        |        ,P.county_code
        |        ,date_format(P.create_time,'YYYY-MM-dd') create_time
        |        ,YEAR ( P.create_time ) YEAR
        |        ,MONTH ( P.create_time ) MONTH
        |        ,DAY ( P.create_time ) DAY
        |        ,IFNULL( prn.is_realname,0 ) is_realname
        |        ,IFNULL( prn2.labourers_count,0 ) labourers_count
        |        ,IFNULL( pro_bank.project_account_status,0 ) project_account_status
        |        ,IFNULL( bank.is_bank_account,0 ) is_bank_account
        |        ,IFNULL( AM.amt,0 ) AS all_amt
        |        ,mcs.safeguard_status
        |        ,IFNULL( mcs.assure_amt,0 ) assure_amt
        |        ,IFNULL( pro_bill.billboards,0 ) is_billboards
        |        ,IFNULL( pe.is_attendance,0 ) is_attendance
        |        ,IFNULL( pro_con.contract_status,0 ) contract_status
        |        ,IFNULL( pta.is_agreement,0 ) is_agreement
        |        ,IFNULL( sb.real_amt,0 ) real_amt
        |        ,IFNULL( tsb.this_month_amt ,0 ) this_month_amt
        |FROM (
        |		select
        |			 P.pid source_pid
        |			,P.pid
        |			,P.cid
        |			,P.project_name
        |			,P.response_code
        |			,P.principal_empid
        |			,P.build_area
        |			,P.construct_cost
        |			,P.project_type
        |			,P.STATUS
        |			,P.is_deleted
        |			,P.project_condition
        |			,P.create_user
        |			,P.province_code
        |			,P.city_code
        |			,P.county_code
        |			,P.create_time
        |		from
        |			ods_gx_project p
        | ) p
        |LEFT JOIN  ods_gx_company  gc
        |ON       p.cid = gc.cid
        |LEFT JOIN coms COM1
        |ON       P.pid = COM1.source_pid AND COM1.company_type=1
        |LEFT JOIN coms COM2
        |ON       P.pid = COM2.source_pid AND COM2.company_type=2
        |LEFT JOIN coms COM3
        |ON       P.pid = COM3.source_pid AND COM3.company_type=3
        |LEFT JOIN coms COM4
        |ON       P.pid = COM4.source_pid AND COM4.company_type=4
        |LEFT JOIN (
        |            SELECT  cl.source_pid
        |                    ,1 AS is_realname
        |            FROM    ods_gx_company_labourer cl
        |            WHERE cl.is_deleted = 1
        |            GROUP BY cl.source_pid
        |          ) prn
        |ON      P.pid = prn.source_pid
        |LEFT JOIN (
        |            SELECT  cl.source_pid
        |                    ,count(DISTINCT cl.lid) labourers_count
        |            FROM    ods_gx_company_labourer cl
        |            WHERE   cl.is_job = true
        |            AND     cl.is_deleted = 1
        |            GROUP BY cl.source_pid
        |          ) prn2
        |ON      P.pid = prn2.source_pid
        |LEFT JOIN (
        |	            select
        |		            pro_all.source_pid
        |		            ,1 as is_bank_account
        |	            from pro_all join  ods_gx_bank_account ba
        |	            on pro_all.pid = ba.pid
        |             and is_deleted=1
        |	            group by pro_all.source_pid
        |) bank on p.pid = bank.source_pid
        |LEFT JOIN AM
        |ON       P.pid = AM.source_pid
        |LEFT JOIN (
        |              SELECT  pro_all.source_pid
        |                      ,1 AS project_account_status
        |              FROM    pro_all join ods_gx_mig_special_account msa
        |              on  pro_all.cid = msa.cid
        |              and msa.is_deleted = 1
        |              AND msa.status = 1
        |              GROUP BY pro_all.source_pid
        |          ) pro_bank
        |ON      P.pid = pro_bank.source_pid
        |LEFT JOIN (
        |            SELECT  pro_all.source_pid
        |                    ,1 AS safeguard_status
        |                    ,sum( mcs.assure_amt ) assure_amt
        |            FROM
        |              pro_all join
        |               (
        |                 SELECT
        |                     mcs.cid
        |                    ,sum( mcs.assure_amt ) assure_amt
        |                 FROM    ods_gx_mig_company_safeguard mcs
        |                 WHERE   mcs.is_deleted = 1
        |                 AND     (mcs.STATUS = 3 OR mcs.STATUS = 4)
        |                 GROUP BY mcs.cid
        |               ) mcs  on  pro_all.cid = mcs.cid
        |                 GROUP BY
        |                   pro_all.source_pid
        |         ) mcs
        |ON      P.pid = mcs.source_pid
        |LEFT JOIN (
        |              SELECT  pb.pid
        |                      ,1 as billboards
        |              FROM    ods_gx_project_billboard pb
        |              WHERE   pb.is_deleted = 1
        |              GROUP BY pb.pid
        |          ) pro_bill
        |ON      P.pid = pro_bill.pid
        |LEFT JOIN (
        |            SELECT  pro_all.source_pid
        |                    ,1 AS is_attendance
        |            FROM    pro_all join ods_gx_project_equipment pe
        |            on      pro_all.pid = pe.pid
        |            and     isdelete = 'N'
        |            AND     is_show = true
        |            GROUP BY pro_all.source_pid
        |          ) pe
        |ON      P.pid = pe.source_pid
        |LEFT JOIN (
        |              SELECT  pro_all.source_pid
        |                      ,1 AS contract_status
        |              FROM    pro_all join  ods_gx_project_attachment gpa
        |              on     pro_all.pid = gpa.pid
        |              and    gpa.is_deleted = 1
        |              AND     (
        |                              gpa.contract1 != ''
        |                          OR  gpa.contract2 != ''
        |                          OR  gpa.contract3 != ''
        |                          OR  gpa.contract4 != ''
        |                          OR  gpa.contract5 != ''
        |                      )
        |              GROUP BY pro_all.source_pid
        |          ) pro_con
        |ON      P.pid = pro_con.source_pid
        |LEFT JOIN (
        |      SELECT  pro_all.source_pid
        |              ,1 AS is_agreement
        |       FROM
        |         pro_all join (
        |            SELECT
        |               pae.pid
        |            FROM    ods_gx_project_tripartite_agreement pae
        |            JOIN    ods_gx_audit ga
        |            ON      pae.pid = ga.bus_id
        |            AND     ga.is_deleted = 1
        |            AND     ga.STATUS = 1
        |            AND     ga.TYPE = 5
        |            WHERE   pae.is_deleted = 1
        |            GROUP BY pae.pid
        |          ) pta on  pro_all.pid = pta.pid
        |          GROUP BY pro_all.source_pid
        |       ) pta
        |ON      P.pid = pta.source_pid
        |LEFT JOIN (
        |              SELECT  SUM(sb.real_amt) real_amt
        |                      ,sb.source_pid
        |              FROM    ods_gx_salary_batch sb
        |              WHERE   sb.STATUS = 1
        |              AND     sb.TYPE = 1
        |              GROUP BY source_pid
        |          ) sb
        |ON      P.pid = sb.source_pid
        |LEFT JOIN (
        |              SELECT  SUM(sb.real_amt) this_month_amt
        |                      ,sb.source_pid
        |              FROM    ods_gx_salary_batch sb
        |              WHERE   sb.STATUS = 1
        |              AND     sb.TYPE = 1
        |              AND     sb.year = year(now())
        |              AND     sb.month = month(now())
        |              GROUP BY source_pid
        |          ) tsb
        |ON      P.pid = tsb.source_pid
        |
        |""".stripMargin

    val sqlstr2=
      """
        | insert overwrite table cdm_gx_project_detail
        | select * from project_detail_view
        |
        |""".stripMargin
    try{

      import spark._
      sql(sqlstr).createOrReplaceTempView("project_detail_view")
      sql(sqlstr2)
    }finally {
      spark.stop()
    }
  }

}

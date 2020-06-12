package hw.cdm.labours

import hw.utils.SparkUtils

/**
 * 统计 员工的全部信息 (包括现在的在职,离职经历)
 *   按照员工是否在职--倒序,员工入职时间--降序 排列;
 *   gcl.is_job DESC, gcl.job_date desc
 *数据来源: ODS层
 *    ods_gx_company_labourer--项目民工表;
 *    ods_gx_labourer--劳务人员表;
 *    ods_gx_project--项目;
 *    ods_gx_dict_area--区域字典;
 *    ods_gx_company--企业;
 *    ods_gx_mig_labourer_black--民工黑名单;
 *    ods_gx_labourer_bank--民工银行卡;
 *    ods_gx_labourer_resume--民工履历表;
 *    cdm_labourer_arrears_detail--欠薪员工明细表;--以移除 2020/4/15 原因:逻辑不严谨 不需要统计了
 * 获取字段:
 *   lid--民工唯一id;
 *   real_name--民工真实名字,
 *   nation_id--民族,
 *   id_card_no--身份证号;
 *   gender--性别(男,女),
 *   birthday--出生日期:年月日,
 *   age--民工年龄,
 *   married--是否结婚(1：是,2：否),
 *   real_grade--认证级别;
 *   province_code --籍贯所在省份,
 *   city_code --籍贯所在城市,
 *   county_code --籍贯所在区县,
 *   culture_level_type --文化程度,
 *          01--小学, 02--初中,03--高中 ,04--中专, 05--大专,06--本科, 07--硕士,08--博士, 99--其他
 *   politics_type --政治面貌,
 *            01--中共党员,02--中共预备党员, 03--共青团员, 04--民革党员,05--民盟盟员,06--民建会员,07--民进会员
 *            08--农工党党员,09--致公党党员,10--九三学社社员,11--台盟盟员,12--无党派人士,13--群众
 *   source_type--实名制来源,
 *          1 实名制客户端，2 中孚 3开发区 4 工汇 99 未知
 * 	 job_date--入职时间,
 * 	 project_province_code--项目所在省份,
 * 	 project_city_code--项目所在城市,
 * 	 current_county_code--当前所在区域编码,
 * 	 current_county_name--当前所在区域名称,
 * 	 project_name--项目名称,
 * 	 project_type--项目类型,
 * 	 company_name--企业名称,
 * 	 worktype_no--当前所做工种的编码,
 * 	 worktype_name--当前所做工种的名称,
 * 	 lab_top   ---获取在职离职员工 以 is_job降序 job_date 降序 排序 获取最新的入职信息
 * 	        1: lab_top=1 and is_job=true 则是 获取最新在职人员所在项目信息;
 * 	        2: is_job=true 则是获取 在职人员所在项目信息 ,人员所在的项目可能有多个信息; 一个人参与多个项目,在职
 *          3: lab_top=1 则获取 实名制民工总人数;
 *   is_job--就职状态(1--在职,0--离职),
 *   bad_grade--民工的不良行为 不良行为等级 1、普通不良行为 2、恶意讨薪 3、不明(其他) 0 无,
 *   bank_no--银行卡号,
 *   bank_name--银行名称,
 *   project_count--民工参与的项目数量
 *   real_amt--欠薪资金金额, --去除统计 2020/4/15
 *   arrears_status--欠薪状态(1--欠薪,0--未欠薪) --去除统计2020/4/15
 *   落地 :
 *    cdm_gx_labourer_resume_detail
 *
 * @author linzhy
 */
object LabourerSalaryDetail {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      """
        |
        |with pro_all as (
        |SELECT
        |	gp.pid source_pid,
        |	gp.project_name source_project_name,
        |	gp.project_type source_project_type,
        |	gp.project_condition source_project_condition,
        |	gp.county_code source_county_code,
        |	gp.cid source_cid,
        |	gp.pid,
        |	gp.project_name,
        |	gp.county_code,
        |	gp.cid
        |FROM
        |	ods_gx_project gp
        |where status in (3,4,5)
        |and is_deleted=1
        |UNION
        | SELECT
        |	  gp.pid source_pid,
        |	  gp.project_name source_project_name,
        |	  gp.project_type source_project_type,
        |	  gp.project_condition source_project_condition,
        |	  gp.county_code source_county_code,
        |   gp.cid source_cid,
        |	  gps.pid,
        |	  gps.project_name,
        | 	gps.county_code,
        |	  gps.cid
        |FROM
        |	  ods_gx_project gp
        | join  ods_gx_project_sub gps on gp.pid = gps.source_pid
        | and gps.is_deleted=1
        | where  gp.is_deleted=1
        | and gp.status in (3,4,5)
        |),
        |salary as (
        |SELECT
        |	sb.pid,
        |	sb.cid,
        |	sb.batch_no,
        |	sb.bkid,
        |	sb.status batch_status,
        |	sb.real_amt batch_real_amt,
        |	sb.amt batch_amt,
        |	sb.number batch_number,
        |	sb.`year`,
        |	sb.`month`,
        |	sb.`year_month`,
        |	sb.create_time batch_create_time,
        |	ss.real_name,
        |	ss.id_card_no,
        |	ss.real_amt,
        | ss.total_amt,
        |	ss.`status`,
        |	ss.worktype_name,
        |	ss.group_name
        |FROM
        |	  ods_gx_salary_batch sb
        |JOIN (
        |     select
        |				ss.batch_no,
        |				ss.real_name,
        |				ss.id_card_no,
        |				ss.real_amt,
        |				ss.total_amt,
        |				ss.`status`,
        |				ps.worktype_name,
        |				ps.group_name
        |			from
        |       ods_gx_salary_sub ss
        |		  left join ods_gx_payroll_sub ps on ss.batch_no = ps.batch_no
        |			and ss.id_card_no = ps.id_card_no
        |			where ss.is_deleted=1
        |) ss ON sb.batch_no = ss.batch_no
        |   WHERE sb.is_deleted =1
        |)
        |insert overwrite table cdm_gx_salary_detail
        |select
        |   p.source_county_code,
        |   p.county_code,
        |   p.county_name,
        |   p.source_pid,
        |	  p.source_project_name,
        |	  p.source_project_type,
        |	  p.source_project_condition,
        |	  p.source_cid,
        |   p.pid,
        |   p.project_name,
        |	  p.cid,
        |	  p.pro_company_name,
        |	  p.pro_company_type,
        |	  sa.cid salary_cid,
        |   coalesce(com.company_type,0) salary_company_type,
        |	  com.company_name salary_company_name,
        |   sa.year,
        |   sa.month,
        |	  sa.`year_month`,
        |   sa.batch_no,
        |	  sa.batch_status,
        |	  sa.batch_real_amt,
        |	  sa.batch_amt,
        | 	sa.batch_number,
        |	  sa.batch_create_time,
        |   sa.id_card_no,
        |   sa.real_name,
        |   sa.real_amt,
        |	  sa.total_amt,
        |   sa.status,
        |	  months_between(now(),sa.`year_month`) new_salary,
        |	  sa.worktype_name,
        |	  sa.group_name,
        |	  ba.bank_name,
        |	  row_number() over (partition by sa.batch_no ) as batch_top,
        |	  dense_rank() over (partition by p.source_pid order by sa.batch_status asc ,sa.year desc,sa.month desc ) salary_top
        |from salary sa join (
        |		select
        |			p.source_pid,
        |			p.pid,
        |			p.source_project_name,
        |			p.project_name,
        |			p.source_project_type,
        |			p.source_county_code,
        |			p.county_code,
        |			p.source_project_condition,
        |			p.cid,
        |			p.source_cid,
        |			com.company_name pro_company_name,
        |			com.company_type pro_company_type,
        |			area.name county_name
        |		from
        |			pro_all p
        |		left join ods_gx_company com on p.cid = com.cid
        |		left join ods_gx_dict_area area on p.county_code = area.code and area.level=3
        |) p on sa.pid = p.pid
        |left join ods_gx_company com on sa.cid = com.cid
        |and com.is_deleted=1
        |left join ods_gx_bank_account ba on sa.bkid = ba.bkid
        |
        |""".stripMargin

    try {

      import spark._
      sql(sqlstr)

    }finally {
      spark.stop()
    }

  }


}

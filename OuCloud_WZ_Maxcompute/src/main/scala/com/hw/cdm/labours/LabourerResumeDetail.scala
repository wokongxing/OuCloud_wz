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
object LabourerResumeDetail {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      """
        |
        |insert overwrite table cdm_gx_labourer_resume_detail
        |SELECT
        | GL.lid,
        | GL.nation_id,
        | GL.real_name,
        | GL.id_card_no,
        | GL.gender,
        | GL.birthday,
        | GL.age,
        | GL.married,
        | GL.mobilephone,
        | GL.real_grade,
        | GL.province_code,
        | GL.city_code,
        | GL.county_code,
        | GL.culture_level_type,
        | GL.politics_type,
        | GL.source_type,
        | CL.job_date,
        | CL.project_province_code,
        | CL.project_city_code,
        | CL.project_county_code,
        | CL.project_name,
        | CL.project_type,
        | CL.company_name,
        | CL.worktype_no,
        | CL.worktype_name,
        | ifnull(CL.lab_top,1) as lab_top,
        | case
        |   when CL.is_job=false  then 0
        |   when CL.is_job is null  then 99
        |   else 1 end as is_job,
        | ifnull(mlb.bad_grade,0) bad_grade,
        | BA.bank_no,
        | BA.bank_name,
        | IFNULL(LR.project_count,0) project_count
        |FROM (
        | select
        |   lid,
        |   nation_id,
        |   real_name,
        |   id_card_no,
        |   gender,
        |   DATE_FORMAT(birthday,'Y-M-d') birthday,
        |   YEAR(now()) - YEAR(birthday) as age,
        |   married,
        |   mobilephone,
        |   real_grade,
        |   province_code,
        |   city_code,
        |   county_code,
        |   culture_level_type,
        |   politics_type,
        |   source_type
        | from
        |	  ods_gx_labourer
        | where is_deleted=1
        | and real_grade != 99
        | ) GL
        |LEFT JOIN (
        |	SELECT
        |		gcl.lid,
        |		gcl.real_name,
        |		gcl.job_date,
        |   c.company_name,
        |   p.project_name,
        |		p.province_code as project_province_code,
        |		p.city_code as project_city_code,
        |		p.county_code as project_county_code,
        |		p.project_type,
        |		gcl.worktype_no,
        |		gcl.worktype_name,
        |		gcl.group_id,
        |		gcl.is_job,
        |		gcl.job_type,
        |		row_number() over( PARTITION BY gcl.lid ORDER BY gcl.is_job DESC, gcl.job_date DESC  ) lab_top
        |	FROM(
        |   select
        |     lid,
        |     source_pid,
        |     current_cid,
        |		  real_name,
        |		  job_date,
        |     worktype_no,
        |		  worktype_name,
        |		  group_id,
        |		  is_job,
        |		  job_type
        |   from
        |     ods_gx_company_labourer
        |   where is_deleted=1
        | ) gcl
        |	LEFT JOIN ods_gx_company c on gcl.current_cid = c.cid
        |	JOIN (
        |		select
        |			pid,
        |     project_name,
        |			province_code,
        |			city_code,
        |			county_code,
        |			project_type
        |		from
        |			ods_gx_project gp
        |		where is_deleted=1
        |	) p on gcl.source_pid=p.pid
        |) CL on GL.lid=CL.lid
        |LEFT JOIN (
        |	select
        |		lid,
        |		bad_grade,
        |		row_number() over (partition by lid order by create_time desc ) black_top
        |	from
        |		ods_gx_mig_labourer_black
        |	where is_deleted=1
        |) mlb on GL.lid = mlb.lid  and black_top=1
        |LEFT JOIN (
        |	select
        |		id_card_no,
        |		bank_no,
        |		bank_name,
        |		row_number() over (partition by id_card_no order by create_time desc ) bank_top
        |	from
        |		ods_gx_labourer_bank
        |	where is_deleted=1
        |) BA ON GL.id_card_no = BA.id_card_no
        |   and BA.bank_top=1
        |LEFT JOIN (
        |	SELECT
        |		lid,
        |		count(DISTINCT source_pid) project_count
        |	FROM
        |		ods_gx_labourer_resume
        |	WHERE is_deleted = 1
        |	GROUP BY lid
        |) LR ON GL.lid=LR.lid
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

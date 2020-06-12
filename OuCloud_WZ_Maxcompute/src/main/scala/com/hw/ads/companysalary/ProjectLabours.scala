package hw.ads.companysalary

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * --项目民工信息表 聚合表
 *  --数据来源:
 *     ads_qx_project;
 *     ads_qx_company_labourer;
 *     ads_qx_labourer;
 *     ads_qx_mig_labourer_black;
 *     ods_base_company;
 *
 *  --获取字段: project_norm_fulfil_new
 * `id` ---------------- 'id',
 * `province_code`-------'所属省',
 * `city_code`-----------'所属市',
 * `lid` ----------------'民工lid',
 * `real_name`-----------'民工姓名',
 * `id_card_no`----------'身份证',
 * `worktype_no` --------'工种编号',
 * `worktype_name`-------'工种名称',
 * `current_pid` --------所属项目pid',
 * `current_project_name` --'所属项目',
 * `current_cid`---------'所属企业cid',
 * `current_company_name` -----'所属企业',
 * `is_job`------------ -'就职状态true在职false离职',
 * `mobilephone`----------'联系方式',
 * `bad_status`-----------'不良行为true有不良行为false不良行为',
 * `create_user`---------- '创建人',
 * `create_time` ----------'创建时间',
 * `last_modify_user`------ '最后修改人',
 * `last_modify_time`-------'最后修改时间',
 * `is_deleted`------------ '是否删除',
 *
 * @author linzhy
 */
object ProjectLabours extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |insert overwrite table ads_qx_cache_labourer
        |  select
        |  L.lid,
        |  qp.industry_type,
        |  qp.project_type,
        |  qp.province_code,
        |  qp.city_code,
        |  qp.county_code,
        |  L.lid,
        |  L.real_name,
        |  L.id_card_no,
        |  L.worktype_no,
        |  L.worktype_name,
        |  L.source_pid as current_pid,
        |  L.source_project_name as current_project_name,
        |  L.current_cid,
        |  coalesce(L.company_name,'--') as current_company_name,
        |  L.is_job,
        |  coalesce(L.mobilephone,'--') mobilephone,
        |  coalesce(L.bad_status,false) bad_status,
        |  '' as create_user,
        |  now() as create_time,
        |  '' as last_modify_user,
        |  now() as last_modify_time,
        |  1 as is_deleted
        |  from
        |  (select pid,province_code,city_code,county_code,project_type,industry_type from ods_base_project where is_deleted=1) qp
        |    join (
        |      select
        |      CL.lid,
        |      coalesce(CL.real_name,L.real_name) real_name,
        |      coalesce(CL.id_card_no,L.id_card_no) id_card_no,
        |      CL.source_pid,
        |      CL.source_project_name,
        |      CL.current_cid,
        |      qc.company_name,
        |      CL.worktype_no,
        |      CL.worktype_name,
        |      CL.is_job,
        |      L.mobilephone,
        |      LB.bad_status,
        |      row_number() over( partition by CL.lid order by CL.is_deleted asc,CL.job_date desc ) rank_top
        |      from
        |      ods_base_company_labourer CL
        |        left join ods_base_labourer L on CL.lid = L.lid
        |        left join (
        |          select
        |          lid,
        |          true as bad_status
        |          from
        |          ods_base_mig_labourer_black
        |          where is_deleted=1
        |          group by lid
        |        ) LB on CL.lid = LB.lid
        |        LEFT JOIN (
        |          select
        |          cid,
        |          company_name
        |          from
        |          ods_base_company
        |          group by cid, company_name
        |        ) qc ON CL.current_cid = qc.cid
        |    ) L on qp.pid = L.source_pid
        |    and L.rank_top=1
        |""".stripMargin

    try{
      import spark._
      sql(sqlstr)

    }finally {
      spark.stop()
    }

  }

}

package hw.ads.gxproject

import hw.utils.SparkUtils


/**
 * 数据来源:
 *     cdm_gx_project_day-以天为维度 -项目信息表;
 *
 * 获取字段:
 * `pid`--------------'项目pid',
 * `cid`--------------'企业cid',
 * `company_name`-----'公司名称',
 * `company_type`-----公司类型 1：建设集团，2：施工单位，3：劳务企业，4：设计单位',
 * `response_code`----'项目代码',
 * `construct_cost`----'项目投资总额',
 * `status`------------'项目审批状态 1：已保存（审核未通过） 2：提交（未审核） 3：待分配（审核通过）4：进行中（已分配）5：完结',
 * `this_month_amt`----'本月发薪金额',
 * `province_code`-----'省code',
 * `city_code`---------'城市code',
 * `county_code`-------'区域code',
 * `labourers_count`----'实名制员工数量',
 * `project_condition`---'项目状态(1-正常,2-停工,3-竣工),'
 *
 * 存储位置:
 *    ads_gx_project_detail_wz--温州总项目数据明细列表
 *
 * @author lzhy
 */
object ProjectDetail {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      """
        |insert overwrite table ads_gx_project_detail
        |	select
        |   pid,
        |   cid,
        |   company_name,
        |   company_type,
        |   response_code,
        |   construct_cost,
        |   status,
        |   this_month_amt,
        |   province_code,
        |   city_code,
        |   county_code,
        |   labourers_count,
        |   project_condition
        |	from
        |		cdm_gx_project_day pd
        | where city_code='330300'
        | and is_deleted=1
        | and status in (3,4,5)
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

package hw.cdm.labours

import hw.utils.SparkUtils

/**
 * 统计
 *    欠薪人数明细统计表
 *      获取项目欠薪人员的明细 --项目区域 项目类型 欠薪金额
 *数据来源: ODS层
 *     ods_gx_mig_arrears_labour 欠薪相关人员表
 *     ods_gx_mig_arrears_salary--企业项目欠薪表 获取区域 欠薪金额
 *            --欠薪状态 3--已确认;4--已处理;
 * 获取字段:
 *   mas.province_code, --------省份
 * 	 mas.city_code, ------------城市
 * 	 mas.county_code,-----------区域
 * 	 mas.project_name,----------项目名称
 * 	 mas.pid,-------------------项目pid
 * 	 mas.project_type,----------项目类型
 * 	 mal.lid,-------------------民工lid
 * 	 mal.real_name,------------民工姓名
 * 	 mal.real_amt,-------------欠薪金额
 *   create_time---------------创建时间
 * --落地
 *    cdm_labourer_arrears_detail
 * @author linzhy
 */
object LabourerArrearsDetail {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)
//    insert overwrite table cdm_labourer_arrears_detail
    val sqlstr =
      """
        |SELECT
        |	mas.province_code,
        |	mas.city_code,
        |	mas.county_code,
        |	mas.project_name,
        |	mas.pid,
        |	mas.project_type,
        |	mal.lid,
        |	mal.real_name,
        |	mal.real_amt,
        | now() create_time
        |FROM (
        |	SELECT
        |		p.province_code,
        |		p.city_code,
        |		p.county_code,
        |		p.project_name,
        |		p.project_type,
        |		mas.id,
        |		p.pid
        |	FROM
        |		ods_gx_mig_arrears_salary mas
        |	join ods_gx_project p on mas.pid = p.pid
        |	and p.is_deleted=1
        |	WHERE mas.STATUS IN(3,4)
        |	AND mas.is_deleted = 1
        |	AND mas.pid != 0
        |) mas
        |JOIN (
        |	SELECT
        |		lid,
        |		real_name,
        |		pid,
        |		arrears_salary_id,
        |		sum( real_amt ) real_amt
        |	FROM
        |		ods_gx_mig_arrears_labour mal
        |	WHERE
        |		mal.is_deleted = 1
        |	GROUP BY
        |		lid,
        |  real_name,
        |		arrears_salary_id,
        |		pid
        |) mal ON mas.id = mal.arrears_salary_id
        |
        |""".stripMargin

    try{
      import spark._
      val dataFrame = sql(sqlstr)
      dataFrame.show(100)
      dataFrame.printSchema()
    }finally {
      spark.stop()
    }

  }


}

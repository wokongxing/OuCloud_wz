package hw.ads.labours

import hw.utils.SparkUtils

/**
 * 统计 员工的全部信息 (包括现在的在职,离职经历)
 *   按照员工是否在职--倒序,员工入职时间--降序 排列;
 *   gcl.is_job DESC, gcl.job_date desc
 *数据来源: ads
 *    cdm_gx_labourer_resume_detail
 * 获取字段:
 *   1: lab_top=1 and is_job=true 则是 获取最新在职人员所在项目信息;
 *   落地 :
 *    cdm_labourer_resume_detail
 * @author linzhy
 */
object LabourerOnJobTotal {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      """
        |SELECT
        |	project_county_code,
        |	project_county_name,
        |	project_type,
        |	COUNT(*) labourer_count
        |FROM
        |	cdm_gx_labourer_resume_detail
        |WHERE lab_top = 1
        |	 AND is_job = 1
        |GROUP BY
        |	project_county_code,
        |	project_county_name,
        |	project_type
        |
        |""".stripMargin

    try {

      import spark._
      val dataFrame = sql(sqlstr)
      dataFrame.show(100)
      dataFrame.printSchema()
    }finally {
      spark.stop()
    }

  }


}

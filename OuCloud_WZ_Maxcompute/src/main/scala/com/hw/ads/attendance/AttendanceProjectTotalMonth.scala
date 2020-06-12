package hw.ads.attendance

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --对考勤平台表 天统计
 *  --数据来源:
 *      cdm_gx_labourer_resume_detail
 *  --获取字段:
 *  存储位置:
 *    ads_gx_attendance_platform_total_day
 *
 * @author linzhy
 */
object AttendanceProjectTotalMonth extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      s"""
        |insert overwrite table ads_gx_attendance_project_total_month
        |select
        |concat(cl.pid,year(date_add(now(), -1)),month(date_add(now(), -1))) as id,
        |  cl.pid,
        |  cl.project_name,
        |  cl.project_province_code,
        |  cl.project_city_code,
        |  cl.project_county_code,
        |  year(date_add(now(), -1)),
        |  month(date_add(now(), -1)),
        |  day(date_add(now(), -1)),
        |  count(id_card_no) labourer_total,
        |  sum(case when is_job=1 then 1 else 0 end ) on_job_count,
        |  sum(case when is_job=0 and year(cl.job_end_date) = year(date_add(now(), -1))
        |  and month(cl.job_end_date) = month(date_add(now(), -1))
        |  then 1 else 0 end ) off_job_count,
        |  now() as process_time,
        |  2 as type
        |from
        |cdm_gx_labourer_resume_detail cl
        |where cl.pid is not null
        |group by cl.project_name,cl.pid,cl.project_province_code,cl.project_city_code,cl.project_county_code
        |
        |""".stripMargin

    try{
      import spark._
      sql(sqlstr)
    }finally {
      spark.stop()
    }

  }

}

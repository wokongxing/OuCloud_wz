package hw.ads.attendance

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --对考勤平台表 年统计
 *  --数据来源:
 *      cdm_gx_labourer_resume_detail
 *  --获取字段:
 *  存储位置:
 *    ads_gx_attendance_platform_total_day
 *
 * @author linzhy
 */
object AttendancePlatformTotalYear extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      s"""
        |
        |insert overwrite table ads_gx_attendance_platform_total_year
        |select
        |  concat( year(date_add(now(), -1)),
        |           coalesce(cl.project_city_code,cl2.project_city_code,0),coalesce(cl.project_county_code,cl2.project_county_code,0),
        |           if (coalesce(cl.nativeplace_id,cl2.nativeplace_id,99) ='',99,coalesce(cl.nativeplace_id,cl2.nativeplace_id,99))) as id,
        |  coalesce(cl.nativeplace_id,cl2.nativeplace_id) nativeplace_id,
        |  coalesce(cl.project_city_code,cl2.project_city_code) project_city_code,
        |  coalesce(cl.project_county_code,cl2.project_county_code) project_county_code,
        |  year(date_add(now(), -1)) date_year,
        |  month(date_add(now(), -1)) date_month,
        |  day(date_add(now(), -1)) date_day,
        |  coalesce(cl.labourer_total,0) labourer_total,
        |  coalesce(cl.onjob_total,0) onjob_total,
        |  coalesce(cl2.offjob_total,0) offjob_total,
        |  now() as process_time,
        |  3 as type
        |from (
        |  select
        |    cl.nativeplace_id,
        |    cl.project_city_code,
        |    cl.project_county_code,
        |    count(*)  as labourer_total,
        |    sum(case when cl.is_job=1 and to_date(cl.job_date) < to_date(now())
        |    then 1 else 0 end ) onjob_total
        |  from
        |  cdm_gx_labourer_resume_detail cl
        |  where cl.lab_top=1
        |  group by cl.nativeplace_id,cl.project_city_code,cl.project_county_code
        |) cl
        |left join (
        |select
        |  cl.nativeplace_id,
        |  cl.project_city_code,
        |  cl.project_county_code,
        |  count(distinct cl.id_card_no) offjob_total
        |from  cdm_gx_labourer_resume_detail cl
        |where cl.is_job=0 and year(cl.job_end_date) = year(date_add(now(), -1))
        |group by cl.nativeplace_id,cl.project_city_code,cl.project_county_code
        |) cl2 on cl.nativeplace_id = cl2.nativeplace_id
        |and cl.project_city_code = cl2.project_city_code
        |and cl.project_county_code = cl2.project_county_code
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

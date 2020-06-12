package hw.cdm.attendance

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --对考勤数据聚合 分区
 *  --数据来源:
 *      t_kq_attendances 考勤全量记录临时表
 *  --获取字段:
 * sn-----------考勤序列号
 * idcardno-----身份证
 * full_name----名字
 * record_time--打卡时间
 *
 * @author linzhy
 */
object AttendanceMonth extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)
    val datetime = spark.conf.get("spark.project.datetime")

    import spark._
    val sqlstr =
      s"""
        |insert overwrite table cdm_kq_attendances_month_p partition (date_year,date_month)
        |SELECT
        |   sn,
        |   idcardno,
        |   trunc(record_time,'MM')  record_time,
        |   year(record_time) date_year,
        |   month(record_time) date_month
        |FROM
        |   t_kq_attendances
        |where idcardno is not null
        |and record_time >=  date_trunc('MM',add_months(now(),${datetime}))
        |GROUP BY
        |   sn,
        |   idcardno,
        |   trunc(record_time,'MM'),
        |   year(record_time) ,
        |   month(record_time)
        |
        |""".stripMargin

    try{
      sql(sqlstr)
    }finally {
      spark.stop()
    }

  }

}

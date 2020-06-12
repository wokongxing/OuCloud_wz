package hw.cdm.attendance

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * --对考勤数据聚合 分区
 *  --数据来源:
 *      t_kq_attendances 考勤全量记录临时表
 *  --获取字段:
 * pid  ------------ '民工id',
 * sn   ----------- '考勤机序列号',
 * direct --------- '进出标记 1--进 /2--出',
 * idcardno --------'身份证号',
 * full_name -------'真实名字',
 * mobile ----------'手机号码',
 * tid  ------------ 'tid',
 * record_time ----- '打卡时间',
 * insert_time ---- '上传时间 ',
 * equp_name -------'设备名称',
 * photo_path ----- '照片保存路径',
 * app_key ---------'app_key'
 *  存储位置: 分区表
 *    cdm_kq_attendances_detail_p
 * @author linzhy
 */
object AttendanceAllMonth extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val startdatetime = spark.conf.get("spark.project.startdatetime")
    val enddatetime = spark.conf.get("spark.project.enddatetime")

    val sqlstr =
      s"""
        |insert overwrite table cdm_kq_attendances_detail_p partition (date_year,date_month)
        |SELECT
        |   pid,
        |   sn,
        |   direct,
        |   idcardno,
        |   full_name,
        |   ifnull(mobile,"无") mobile,
        |   ifnull(tid,'无') tid,
        |   record_time,
        |   ifnull(insert_time,record_time) insert_time,
        |   ifnull(equp_name,'无') equp_name,
        |   ifnull(photo_path,'无') photo_path,
        |   ifnull(app_key,'无') app_key,
        |   ifnull(year(record_time),2015) date_year,
        |   ifnull(month(record_time),1) date_month
        |FROM
        |   t_kq_attendances
        |where idcardno is not null
        |and record_time is not null
        |and record_time >=  date_trunc('MM',add_months(now(),${startdatetime}))
        |and record_time <  date_trunc('MM',add_months(now(),${enddatetime}))
        |""".stripMargin
    try{
      import spark._
      sql(sqlstr)
    }finally {
      spark.stop()
    }

  }

}

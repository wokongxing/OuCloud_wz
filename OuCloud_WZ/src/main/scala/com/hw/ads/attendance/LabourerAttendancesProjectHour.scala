package main.scala.com.hw.ads.attendance

import com.typesafe.config.ConfigFactory
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.SparkSession

/**
 * 需求:
 *    获取打卡的 项目名称 区域, 时间--(年-月-日 -小时-分钟),人数
 * 数据来源:
 *    ODS层 考勤数据表 kq_attendances 从es 增量拉取
 *  条件:
 *    direct (1--进入上班, 2--下班 出)
 * 获取字段:
 *  id--唯一rowkey值,
 *  pid--考勤中人员id,
 *  full_name --真实名字,
 *  idcardno --身份证,
 *  insert_time --上传数据到考勤机上的时间,
 *  record_date --考勤打卡时间,
 *  record_year --考勤年份,
 *  record_month --考勤月份,
 *  record_day --考勤的天,
 *  in_sn -- 上班打卡的考勤机序列号,
 *  in_record_hour --上班打卡时间点 --小时,
 *  in_record_minute --上班打卡时间点 --分钟,
 *  out_sn --下班打卡的考勤机序列号,
 *  out_record_hour 下班打卡时间点 --小时,
 *  out_record_minute --下班打卡时间点 --分钟,
 *  worktime --工作时长,
 *  attendance_status -- 考勤状态 (1--异常,0--正常)
 *
 *
 * @author linzhy
 */
object LabourerAttendancesProjectHour {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer") //序列化
      .getOrCreate()

    val config = ConfigFactory.load()
    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    //获取考勤信息
    val kq_attendances = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "kq_attendances")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    kq_attendances.createOrReplaceTempView("kq_attendances")

    val sql =
      """
        |
        |WITH on_work AS (
        | SELECT * FROM (
        | SELECT
        |   AT.sn,
        |   AT.pid,
        |		AT.direct,
        |		AT.full_name,
        |		AT.idcardno,
        |		AT.record_time,
        |		AT.insert_time,
        |		date(AT.record_time) record_date,
        |		year( AT.record_time) record_year,
        |		month( AT.record_time) record_month,
        |		day( AT.record_time) record_day,
        |   hour(AT.record_time) record_hour,
        |   minute(AT.record_time) record_minute,
        |		ROW_NUMBER () OVER ( PARTITION BY AT.pid, date_format( AT.record_time, 'Y-M-d' ) ORDER BY AT.record_time ASC ) RANK_TOP
        |	FROM
        |		kq_attendances AT
        |	WHERE
        |		AT.direct = 1
        | ) AT WHERE AT.RANK_TOP = 1  ORDER BY AT.record_time DESC
        |),
        | off_work AS (
        |  SELECT
        |	    *
        |  FROM
        |	(
        |	SELECT
        |		AT.sn,
        |		AT.pid,
        |		AT.direct,
        |		AT.full_name,
        |		AT.idcardno,
        |		AT.record_time,
        |		AT.insert_time,
        |		date(AT.record_time) record_date,
        |		year( AT.record_time) record_year,
        |		month( AT.record_time) record_month,
        |		day( AT.record_time) record_day,
        |   hour(AT.record_time) record_hour,
        |   minute(AT.record_time) record_minute,
        |		ROW_NUMBER () OVER ( PARTITION BY AT.pid, date_format( AT.record_time, 'Y-M-d' ) ORDER BY AT.record_time DESC ) RANK_TOP
        |	FROM
        |		kq_attendances AT
        |	WHERE
        |		AT.direct = 2
        |	) AT WHERE AT.RANK_TOP = 1 ORDER BY AT.record_time DESC
        |)
        |SELECT
        | concat(IFNULL(W1.pid,W2.pid) ,IFNULL(W1.record_year,W2.record_year) ,IFNULL(W1.record_month,W2.record_month) ,IFNULL(W1.record_day,W2.record_day) ) id,
        | IFNULL(W1.pid,W2.pid) pid,
        | IFNULL(W1.full_name,W2.full_name) full_name,
        | IFNULL(W1.idcardno,W2.idcardno) idcardno,
        | IFNULL(W1.insert_time,W2.insert_time) insert_time,
        | IFNULL(W1.record_date,W2.record_date) record_date,
        | IFNULL(W1.record_year,W2.record_year) record_year,
        | IFNULL(W1.record_month,W2.record_month) record_month,
        | IFNULL(W1.record_day,W2.record_day) record_day,
        | W1.sn in_sn,
        | W1.record_hour in_record_hour,
        | W1.record_minute in_record_minute,
        | W2.sn out_sn,
        | W2.record_hour out_record_hour,
        | W2.record_minute out_record_minute,
        | ROUND ( ( unix_timestamp( W2.record_time ) - unix_timestamp( W1.record_time ) )/3600 , 1 ) as worktime,
        | CASE WHEN
        |   W1.sn is null
        |   or W2.sn is null
        |   or (unix_timestamp( W2.record_time ) - unix_timestamp( W1.record_time )) < 0
        | then 1 else 0 end attendance_status
        |FROM
        |   on_work  W1 FULL OUTER JOIN off_work W2
        |ON W1.pid = W2.pid
        |AND W1.record_date = W2.record_date
        |where concat(IFNULL(W1.pid,W2.pid) ,IFNULL(W1.record_year,W2.record_year) ,IFNULL(W1.record_month,W2.record_month) ,IFNULL(W1.record_day,W2.record_day) ) !=null
        |""".stripMargin

    val dataFrame = spark.sql(sql)
    dataFrame.printSchema()
    val conn = PgSqlUtil.connectionPool("OuCloud_CDM")
    PgSqlUtil.insertOrUpdateToPgsql(conn,dataFrame,spark.sparkContext,"cdm_attendance","id")
    dataFrame.show(1000)

    spark.stop()

  }
}

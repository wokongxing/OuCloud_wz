package hw.cdm.attendance

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --对考勤明细聚合表 信息补充
 *  --数据来源:
 *      t_kq_attendances
 *      ods_gx_project
 *      ods_gx_project_equipment
 *  --获取字段:
 *  存储位置:
 *    cdm_gx_project_attendance_detail_p
 *
 * @author linzhy
 */
object AttendanceProjectDetail extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      s"""
        |
        |WITH on_work AS (
        |  SELECT * FROM (
        |    SELECT
        |      AT.sn,
        |      AT.pid,
        |      AT.direct,
        |      AT.full_name,
        |      AT.idcardno,
        |      AT.record_time,
        |      coalesce(AT.insert_time,AT.record_time) insert_time,
        |      date_format( AT.record_time, 'yyyy-MM-dd' ) record_date,
        |      year( AT.record_time) record_year,
        |      month( AT.record_time) record_month,
        |      day( AT.record_time) record_day,
        |      hour(AT.record_time) record_hour,
        |      minute(AT.record_time) record_minute,
        |      ROW_NUMBER () OVER ( PARTITION BY AT.pid, date_format( AT.record_time, 'yyyy-MM-dd' ) ORDER BY AT.record_time ASC ) RANK_TOP
        |    FROM
        |       cdm_kq_attendances_detail_p AT
        | WHERE date_year = year(date_add(now(),-1))
        | and date_month >= month(add_months(date_add(now(),-1),-1))
        | and AT.direct = 1
        |  ) AT WHERE AT.RANK_TOP = 1
        |),
        |off_work AS (
        |  SELECT
        |         *
        |  FROM
        |  (
        |   SELECT
        |     AT.sn,
        |     AT.pid,
        |     AT.direct,
        |     AT.full_name,
        |     AT.idcardno,
        |     AT.record_time,
        |     coalesce(AT.insert_time,AT.record_time) insert_time,
        |     date_format( AT.record_time, 'yyyy-MM-dd' ) record_date,
        |     year( AT.record_time) record_year,
        |     month( AT.record_time) record_month,
        |     day( AT.record_time) record_day,
        |     hour(AT.record_time) record_hour,
        |     minute(AT.record_time) record_minute,
        |     ROW_NUMBER () OVER ( PARTITION BY AT.pid, date_format( AT.record_time, 'yyyy-MM-dd' ) ORDER BY AT.record_time DESC ) RANK_TOP
        | FROM
        |   cdm_kq_attendances_detail_p AT
        | WHERE date_year = year(date_add(now(),-1))
        | and date_month >= month(add_months(date_add(now(),-1),-1))
        | and AT.direct = 2
        |  ) AT WHERE AT.RANK_TOP = 1
        |)
        |insert overwrite table cdm_gx_project_attendance_detail_p partition (date_year,date_month)
        |select
        |  at.id,
        |  at.full_name,
        |  at.idcardno,
        |  at.insert_time,
        |  at.record_date,
        |  at.record_year,
        |  at.record_month,
        |  at.record_day,
        |  at.sn,
        |  at.record_time,
        |  at.in_sn,
        |  at.in_record_hour,
        |  at.in_record_minute,
        |  at.out_sn,
        |  at.out_record_hour,
        |  at.out_record_minute,
        |  at.worktime,
        |  at.attendance_status,
        |  pe.pid,
        |  pe.project_name,
        |  pe.province_code,
        |  pe.city_code,
        |  pe.county_code,
        |  pe.project_condition,
        |  pe.project_type,
        |  pe.equipmentname,
        |  pe.dev_type,
        |  pe.dev_manufacturer,
        |  pe.create_time,
        |  pe.last_modify_time,
        |  pe.isdelete,
        |  pe.is_show,
        |  pe.is_enable,
        |  at.record_year date_year,
        |  at.record_month date_month
        |from (
        |  select
        |    p.pid,
        |    p.project_name,
        |    p.province_code,
        |    p.city_code,
        |    p.county_code,
        |    p.project_condition,
        |    p.project_type,
        |    pe.sn,
        |    pe.equipmentname,
        |    pe.dev_type,
        |    pe.dev_manufacturer,
        |    pe.create_time,
        |    pe.last_modify_time,
        |    pe.isdelete,
        |    pe.is_show,
        |    pe.is_enable
        |  from
        |    ods_gx_project_equipment pe join ods_gx_project p
        |  on pe.pid = p.pid and p.is_deleted=1
        |) pe join (
        |  SELECT
        |    concat(coalesce(W1.pid,W2.pid) ,coalesce(W1.record_year,W2.record_year) ,coalesce(W1.record_month,W2.record_month) ,coalesce(W1.record_day,W2.record_day) ) id,
        |    coalesce(W1.pid,W2.pid) pid,
        |    coalesce(W1.full_name,W2.full_name) full_name,
        |    coalesce(W1.idcardno,W2.idcardno) idcardno,
        |    coalesce(W1.insert_time,W2.insert_time) insert_time,
        |    coalesce(W1.record_date,W2.record_date) record_date,
        |    coalesce(W1.record_year,W2.record_year) record_year,
        |    coalesce(W1.record_month,W2.record_month) record_month,
        |    coalesce(W1.record_day,W2.record_day) record_day,
        |    coalesce(W1.sn,W2.sn) sn,
        |    coalesce(W1.record_time,W2.record_time) record_time,
        |    W1.sn in_sn,
        |    W1.record_hour in_record_hour,
        |    W1.record_minute in_record_minute,
        |    W2.sn out_sn,
        |    W2.record_hour out_record_hour,
        |    W2.record_minute out_record_minute,
        |     CASE
        |       WHEN W1.sn is null or W2.sn is null then 0
        |    else  ROUND ( ( unix_timestamp( cast (W2.record_time as String),'yyyy-MM-dd HH:mm:ss') - unix_timestamp( cast (W1.record_time as String),'yyyy-MM-dd HH:mm:ss') )/3600 , 1 )
        |     end as worktime,
        |    CASE
        |    WHEN W1.sn is null or W2.sn is null
        |    or ( unix_timestamp( cast (W2.record_time as String),'yyyy-MM-dd HH:mm:ss') - unix_timestamp( cast (W1.record_time as String),'yyyy-MM-dd HH:mm:ss') ) < 0
        |    then 0 else 1
        |    end as attendance_status
        |  FROM
        |  on_work  W1 FULL OUTER JOIN off_work W2 ON W1.pid = W2.pid
        |  AND W1.record_date = W2.record_date
        |) at on pe.sn = at.sn
        |where  datediff(at.record_time,pe.create_time) > 0
        |and if (pe.isdelete='N',1,datediff(pe.last_modify_time,at.record_time)) >0
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

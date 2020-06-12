package hw.ads.attendance

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --实名制-考勤表年月
 *  --数据来源:
 *      cdm_kq_attendances_month_p;
 *      ods_base_project_equipment;
 *      ods_base_labourer_resume
 *  --获取字段:
 * `id
 * `pid----------项目id',
 * `status-------状态：1，未落实  2，已处理  3，已落实',
 * `cur_date------所属年月日期',
 * `cur_date_year--所属年',
 * `cur_date_month--所属月',
 * `real_count` -----'实名制人数',
 * `attendance_count-------考勤人数',
 * `is_exception-----------是否异常(1，正常 2，异常)',
 * `create_user----创建人',
 * `create_time----创建时间',
 * `last_modify_user---修改人',
 * `last_modify_time----修改时间',
 * `is_deleted-------是否删除 - 1:未删除 2:删除'
 *
 * @author linzhy
 **/
object ProjectInfoRealAttendance extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)


    //获取需要统计的时间
    val dateMaps = spark.conf.get("spark.project.dateMaps")
    val dates = dateMaps.split(",")
    var real_sql = new StringBuffer()

    import spark._

    if (dates.size>1) {
      dateMaps.split(",").map(x => {
        val datetime = x
        var sqlstr =
          s"""
             |select
             |  concat(qp.pid,year('${datetime}'),month('${datetime}')) id,
             |  qp.pid,
             |  trunc('${datetime}','MM')  cur_date,
             |  year('${datetime}')  cur_date_year,
             |  month('${datetime}') cur_date_month,
             |  ifnull (lr.real_count,0) real_count,
             |  ifnull (a.attendance_count,0) attendance_count,
             |  1 as is_exception,
             |  '' create_user,
             |  now() create_time,
             |  '' last_modify_user,
             |  now() last_modify_time,
             |  qp.is_deleted
             |from
             |  ods_base_project qp
             |LEFT JOIN (
             |	SELECT
             |		source_pid,
             |		COUNT( DISTINCT id_card_no) as real_count
             |	FROM
             |		ods_base_labourer_resume
             |	WHERE is_deleted = 1
             |	AND endtime >= date_format ( '${datetime}', 'yyyy-MM-dd HH:mm:ss' )
             |	AND starttime < date_format ( add_months ( '${datetime}', 1 ), 'yyyy-MM-dd HH:mm:ss' )
             |	GROUP BY
             |		source_pid
             |) lr ON lr.source_pid = qp.pid
             |LEFT JOIN(
             |  SELECT
             |	  pe.pid,
             |	  COUNT( DISTINCT pl.idcardno) as attendance_count
             |  FROM
             |	  ods_base_project_equipment pe
             |  LEFT JOIN (
             |		SELECT
             |			ap.sn,
             |			ap.idcardno
             |		FROM
             |			(select * from cdm_kq_attendances_month_p ap  where ap.date_year = year('${datetime}') AND ap.date_month = month('${datetime}')) ap
             |		JOIN (
             |			SELECT
             |				DISTINCT id_card_no
             |			FROM
             |				ods_base_labourer_resume
             |			WHERE is_deleted = 1
             |			AND endtime >= date_format ( '${datetime}', 'yyyy-MM-dd HH:mm:ss' )
             |			AND starttime < date_format ( add_months ( '${datetime}', 1 ), 'yyyy-MM-dd HH:mm:ss' )
             |		) lr ON ap.idcardno = lr.id_card_no
             |  ) pl on pe.sn = pl.sn
             |	where pe.isdelete='N'
             |	group by pe.pid
             |) a on qp.pid = a.pid
             |
             |""".stripMargin
        real_sql.append(sqlstr).append("union")
      })
    }else {
      val datetime = dateMaps
      var sqlstr =
        s"""
           |select
           |  concat(qp.pid,year('${datetime}'),month('${datetime}')) id,
           |  qp.pid,
           |  trunc('${datetime}','MM')  cur_date,
           |  year('${datetime}')  cur_date_year,
           |  month('${datetime}') cur_date_month,
           |  ifnull (lr.real_count,0) real_count,
           |  ifnull (a.attendance_count,0) attendance_count,
           |  1 as is_exception,
           |  '' create_user,
           |  now() create_time,
           |  '' last_modify_user,
           |  now() last_modify_time,
           |  qp.is_deleted
           |from
           |  ods_base_project qp
           |LEFT JOIN (
           |	SELECT
           |		source_pid,
           |		COUNT( DISTINCT id_card_no) as real_count
           |	FROM
           |		ods_base_labourer_resume
           |	WHERE is_deleted = 1
           |	AND endtime >= date_format ( '${datetime}', 'yyyy-MM-dd HH:mm:ss' )
           |	AND starttime < date_format ( add_months ( '${datetime}', 1 ), 'yyyy-MM-dd HH:mm:ss' )
           |	GROUP BY
           |		source_pid
           |) lr ON lr.source_pid = qp.pid
           |LEFT JOIN(
           |  SELECT
           |	  pe.pid,
           |	  COUNT( DISTINCT pl.idcardno) as attendance_count
           |  FROM
           |	  ods_base_project_equipment pe
           |  LEFT JOIN (
           |		SELECT
           |			ap.sn,
           |			ap.idcardno
           |		FROM
           |			(select * from cdm_kq_attendances_month_p ap  where ap.date_year = year('${datetime}') AND ap.date_month = month('${datetime}')) ap
           |		JOIN (
           |			SELECT
           |				DISTINCT id_card_no
           |			FROM
           |				ods_base_labourer_resume
           |			WHERE is_deleted = 1
           |			AND endtime >= date_format ( '${datetime}', 'yyyy-MM-dd HH:mm:ss' )
           |			AND starttime < date_format ( add_months ( '${datetime}', 1 ), 'yyyy-MM-dd HH:mm:ss' )
           |		) lr ON ap.idcardno = lr.id_card_no
           |  ) pl on pe.sn = pl.sn
           |	where pe.isdelete='N'
           |	group by pe.pid
           |) a on qp.pid = a.pid
           |
           |""".stripMargin
      real_sql.append(sqlstr).append("union")
    }
    val sql2 =
      """
        |insert overwrite table ads_qx_project_info_real_attendance_p partition (date_year,date_month)
        |select
        | id,
        | pid,
        | cur_date,
        | cur_date_year,
        | cur_date_month,
        | case when ifnull(attendance_count,0) > 0 then 3 else 1 end status,
        | ifnull(real_count,0) real_count,
        | ifnull(attendance_count,0) attendance_count,
        | is_exception,
        | create_user,
        | create_time,
        | last_modify_user,
        | last_modify_time,
        | is_deleted,
        | cur_date_year AS date_year,
        | cur_date_month AS date_month
        |from temp_project_info_real_attendance
        |
        |""".stripMargin

    try{

      val sqlstr = real_sql.substring(0,real_sql.length()-5)
      logInfo(sqlstr)
      sql(sqlstr).createOrReplaceTempView("temp_project_info_real_attendance")
      sql(sql2)
    }finally {
      spark.stop()
    }

  }

}

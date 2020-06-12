package hw.ads.attendance

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --对考勤设备表 天统计
 *  --数据来源:
 *      ods_gx_project_equipment
 *      ods_gx_project
 *      cdm_kq_attendances_detail_p
 *  --获取字段:
 *  存储位置:
 *    cdm_gx_project_equipment_detail
 *
 * @author linzhy
 */
object AttendanceEquipmentTotalDay extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      s"""
        |with
        |sn_total as (
        |  SELECT
        |    pe.county_code,
        |    count(distinct sn) as sn_total_count,
        |    sum(day_status) as sn_online_count,
        |    sum(case when pe.dev_manufacturer='中控' then 1 else 0 end ) as zk_sn_count,
        |    sum(case when pe.dev_manufacturer='海康' then 1 else 0 end ) as hk_sn_count,
        |    sum(case when pe.dev_manufacturer='海清视讯' then 1 else 0 end ) as hqsx_sn_count,
        |    sum(case when pe.dev_manufacturer='赤道' then 1 else 0 end ) as cd_sn_count,
        |    sum(case when pe.dev_manufacturer='其他' then 1 else 0 end ) as other_sn_count
        |  FROM
        |    cdm_gx_project_equipment_detail pe
        |  WHERE  datediff(pe.last_modify_time,date_add(now(), -1)) <= 0
        |  and 	isdelete = 'N'
        |  group by pe.county_code
        |),
        |install_sn as (
        |  SELECT
        |    pe.county_code,
        |    count(distinct sn) as sn_install_count
        |  FROM
        |    cdm_gx_project_equipment_detail pe
        |  WHERE  	YEAR(create_time) = YEAR(date_add(now(), -1))
        |  and  	MONTH(create_time) = MONTH(date_add(now(), -1))
        |  and  	day(create_time) = day(date_add(now(), -1))
        |  and 	pe.isdelete = 'N'
        |  group by pe.county_code
        |),
        |dismantle_sn as (
        | SELECT
        |    pe.county_code,
        |    count(distinct sn) as sn_dismantle_count
        | FROM
        |    cdm_gx_project_equipment_detail pe
        |  WHERE  	YEAR(last_modify_time) = YEAR(date_add(now(), -1))
        |  and  	MONTH(last_modify_time) = MONTH(date_add(now(), -1))
        |  and  	day(last_modify_time) = day(date_add(now(), -1))
        |  and 	isdelete = 'Y'
        |  group by pe.county_code
        |)
        |insert overwrite table ads_gx_attendance_equipment_total_day
        |select
        |  concat(area.code,year(date_add(now(), -1)),month(date_add(now(), -1)), day(date_add(now(), -1))) as id,
        |  area.code,
        |  area.name,
        |  year(date_add(now(), -1)) year_time,
        |  month(date_add(now(), -1)) month_time,
        |  day(date_add(now(), -1)) day_time,
        |  coalesce(a.sn_total_count,0) sn_total_count,
        |  coalesce(a.sn_online_count,0) sn_online_count,
        |  coalesce(a.zk_sn_count,0) zk_sn_count,
        |  coalesce(a.hk_sn_count,0) hk_sn_count,
        |  coalesce(a.hqsx_sn_count,0) hqsx_sn_count,
        |  coalesce(a.cd_sn_count,0) cd_sn_count,
        |  coalesce(a.other_sn_count,0) other_sn_count,
        |  coalesce(a.sn_install_count,0) sn_install_count,
        |  coalesce(a.sn_dismantle_count,0) sn_dismantle_count,
        |  date_add(now(), -1) as process_time,
        |  1 as type
        |from (select code ,name from ods_gx_dict_area area where area.parentcode='330300') area left join
        |(
        |  select
        |    coalesce(a1.county_code,a2.county_code,a3.county_code) county_code,
        |    a1.sn_total_count,
        |    a1.sn_online_count,
        |    a1.zk_sn_count,
        |    a1.hk_sn_count,
        |    a1.hqsx_sn_count,
        |    a1.cd_sn_count,
        |    a1.other_sn_count,
        |    a2.sn_install_count,
        |    a3.sn_dismantle_count
        |  from
        |    sn_total a1
        |  full outer join install_sn a2 on a1.county_code = a2.county_code
        |  full outer join dismantle_sn a3 on a1.county_code = a3.county_code
        |) a on area.code = a.county_code
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

package hw.cdm.attendance

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --对考勤设备表 信息补充
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
object AttendanceEquipmentDetail extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      s"""
        |
        |insert overwrite table cdm_gx_project_equipment_detail
        |select
        |pe.id,
        |pro.pid,
        |pro.project_name,
        |pro.province_code,
        |pro.city_code,
        |pro.county_code,
        |pro.project_condition,
        |pe.sn,
        |pe.equipmentname,
        |pe.dev_type,
        |pe.isdelete,
        |pe.create_time,
        |pe.create_user,
        |pe.last_modify_time,
        |pe.last_modify_user,
        |pe.dev_manufacturer,
        |pe.is_show,
        |pe.is_enable,
        |coalesce(ka.day_status,0) day_status,
        |coalesce(ka2.month_status,0) month_status,
        |coalesce(ka3.year_status,0) year_status
        |from
        |ods_gx_project_equipment  pe
        |  join (
        |    SELECT
        |       gp.pid,
        |       gp.project_name,
        |       gp.province_code,
        |       gp.city_code,
        |        gp.county_code,
        |       gp.project_condition
        |    FROM
        |       ods_gx_project gp
        |    where status in (3,4,5)
        |    and is_deleted=1
        |  ) pro on pro.pid = pe.pid
        |  left join(
        |    select
        |       sn,
        |       1 as day_status
        |    from
        |       cdm_kq_attendances_detail_p kap
        |    where kap.date_year=2020
        |    and kap.date_month=5
        |    and to_date(record_time) = to_date(date_add(now(), -1))
        |    GROUP BY SN
        |  ) ka on pe.sn = ka.sn
        |  left join(
        |    select
        |       sn,
        |       1 as month_status
        |    from
        |       cdm_kq_attendances_detail_p kap
        |    where  kap.date_year=year(date_add(now(), -1))
        |    and kap.date_month=month(date_add(now(), -1))
        |    GROUP BY SN
        |  ) ka2 on pe.sn = ka2.sn
        |  left join(
        |    select
        |       sn,
        |       1 as year_status
        |    from
        |       cdm_kq_attendances_detail_p kap
        |    where kap.date_year=year(date_add(now(), -1))
        |    GROUP BY SN
        |  ) ka3 on pe.sn = ka3.sn
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

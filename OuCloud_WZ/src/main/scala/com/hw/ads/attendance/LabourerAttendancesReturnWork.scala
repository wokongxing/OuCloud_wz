package main.scala.com.hw.ads.attendance

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.{DataBaseName, PgSqlUtil}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 需求:
 *    2.4 号复工人员的统计
 * 数据来源:
 *    ODS层:
 *      考勤数据表 cdm_attendances 从es 增量拉取
 *    CDM层:
 *      gx_labourer --劳务员工表
 *      gx_salary_sub --发薪明细表
 *      gx_project--源项目
 *      gx_project_sub--子项目信息
 *      aj_dict_area --区域表
 *  条件:
 *    1--2.4号之后有考勤记录 --数据来源:es中 2.4号有考勤记录;
 *    2--该人员已实名, --数据来源:--工薪--labourer表; 关联:idcardno
 *    3--2月份有发放薪资记录 --数据来源:-- salary_sub发薪明细表  关联:idcardno
 * 获取字段:
 *  full_name --真实名字,
 *  idcardno --身份证,
 *  attendance_status --考勤状态,
 *  is_realname --实名制 1--是,0--否,
 *  salary_status --考勤年份,
 *  project_name --项目时间,
 *  project_condition --项目状态,
 *  county_name --项目区域
 *
 * @author linzhy
 */
object LabourerAttendancesReturnWork {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer") //序列化
      .getOrCreate()

    val config = ConfigFactory.load()
    queryDataCreateView(spark,config)

    val sql =
      """
        |WITH AT AS (
        |   SELECT
        |	    full_name,
        |	    idcardno,
        |     ifnull (in_sn,out_sn) sn
        |   FROM
        |	    cdm_attendance
        |   WHERE record_year = 2020
        | 	  AND record_month >= 2
        |	    AND record_day >= 4
        |     and rank =1
        | ),
        | LAB AS (
        |   SELECT
        |     id_card_no,
        |     1 as is_realname
        |   FROM
        | 	  gx_labourer l
        |   WHERE l.real_grade != 99
        |   AND l.is_deleted =1
        | ),
        | PRO AS (
        |   SELECT
        |	    gp.pid source_pid,
        |		  gp.pid,
        |		  gp.project_name,
        |	    gp.county_code,
        |	    gp.project_condition,
        |		  status,
        |		  is_deleted,
        |		  city_code
        |   FROM
        |	    gx_project gp
        |		where status in (3,4,5)
        |		  and is_deleted=1
        |UNION
        | SELECT
        |	  gp.pid source_pid,
        |	  gps.pid,
        |		gp.project_name,
        |	  gp.county_code,
        |	  gp.project_condition,
        |		gp.status,
        |		gp.is_deleted,
        |		gp.city_code
        |FROM
        |	  gx_project gp
        | join
        |	  gx_project_sub gps on gp.pid = gps.source_pid
        |		and gps.is_deleted=1
        |		where  gp.is_deleted=1
        |		and gp.status in (3,4,5)
        | ),
        | SAL AS (
        |     SELECT
        |	      ss.pid,
        |	      ss.id_card_no,
        |	      ss.create_time,
        |       1 AS salary_status,
        |	      row_number() over(partition by ss.id_card_no order by ss.create_time desc ) rank
        |    FROM
        |	      gx_salary_sub ss
        |     WHERE	ss.create_time >'2020-02-01'
        |	      AND ss.status = 1
        |	      AND ss.is_labourer = 1
        |	      AND ss.is_deleted = 1
        | ),
        | PE AS (
        | select
        |   *
        | from (
        |    SELECT
        |	      pid,
        |	      project_name,
        |	      sn,
        |	      row_number() over ( partition by sn order by create_time desc ) rank
        |     FROM
        |	      gx_project_equipment
        |     WHERE isdelete = 'N'
        |	      AND is_show = TRUE
        |   	  AND is_enable = TRUE
        |  ) pe where pe.rank=1
        | )
        | SELECT
        |   AT.idcardno,
        |   AT.full_name,
        |   1 AS attendance_status,
        |   IFNULL(LAB.is_realname,0) is_realname,
        |   IFNULL(SAL.salary_status,0) salary_status,
        |   PES.project_name,
        |   PES.project_condition,
        |   PES.county_name
        | FROM
        |   AT
        | LEFT JOIN LAB ON upper(AT.idcardno) = upper(LAB.id_card_no)
        | LEFT JOIN SAL ON upper(AT.idcardno) = upper(SAL.id_card_no) AND SAL.rank=1
        | LEFT JOIN (
        |     select
        |       PE.sn,
        |       PRO.project_name,
        |       PRO.project_condition,
        |       area.name county_name
        |     from
        |       PE
        |     LEFT JOIN PRO ON PE.pid = PRO.pid
        |     LEFT JOIN aj_dict_area area ON PRO.county_code = area.code
        | ) PES ON AT.sn = PES.sn
        |
        |""".stripMargin

    val dataFrame = spark.sql(sql)

    dataFrame.printSchema()
    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"ads_attendance_returnwork_total")
//    val conn = PgSqlUtil.connectionPool(DataBaseName.OuCloud_ADS.toString)
//    PgSqlUtil.insertOrUpdateToPgsql(conn,dataFrame,spark.sparkContext,"ads_attendance_returnwork_total","idcardno")
//    dataFrame.show(1000)

    spark.stop()

  }


  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")
    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")

    val gx_labourer = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_labourer")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    val gx_salary_sub = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_salary_sub")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    val gx_project = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_project")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    val gx_project_sub = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_project_sub")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    val gx_project_equipment = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_project_equipment")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //获取区域字典
    val aj_dict_area = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","aj_dict_area")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    //获取考勤信息
    val cdm_attendance = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "cdm_attendance")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    cdm_attendance.createOrReplaceTempView("cdm_attendance")
    gx_labourer.createOrReplaceTempView("gx_labourer")
    gx_salary_sub.createOrReplaceTempView("gx_salary_sub")
    gx_project.createOrReplaceTempView("gx_project")
    gx_project_sub.createOrReplaceTempView("gx_project_sub")
    gx_project_equipment.createOrReplaceTempView("gx_project_equipment")
    aj_dict_area.where("parent_code='330300' and level=3")createOrReplaceTempView("aj_dict_area")




  }

}

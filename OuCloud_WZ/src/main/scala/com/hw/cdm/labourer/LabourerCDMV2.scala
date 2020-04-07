package main.scala.com.hw.cdm.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 统计 员工的全部信息 (包括现在的在职,离职经历)
 *   按照员工是否在职--倒序,是否删除数据--升序,员工入职时间--升序 排列;
 *   gcl.is_job DESC,gcl.is_deleted ASC, gcl.create_time ASC
 *数据来源: ODS层
 *  gx_company_labourer--项目民工表;
 *  gx_labourer--劳务人员表;
 *  gx_mig_arrears_labour--欠薪员工表;
 *  aj_special_employee--安监特种工表;
 *  aj_employees--安监普通工员信息表;
 * 获取字段:
 *   lid--民工唯一id;
 *   real_name--民工真实名字,
 *   nation_id--民族,
 *   id_card_no--身份证号;
 *   gender--性别(男,女),
 *   birthday--出生日期:年月日,
 *   age--民工年龄,
 *   married--是否结婚(1：是,2：否),
 *   real_grade--认证级别;
 *   province_code --籍贯所在省份,
 *   city_code --籍贯所在城市,
 *   county_code --籍贯所在区县,
 *   culture_level_type --文化程度,
 *          01--小学, 02--初中,03--高中 ,04--中专, 05--大专,06--本科, 07--硕士,08--博士, 99--其他
 *   politics_type --政治面貌,
 *            01--中共党员,02--中共预备党员, 03--共青团员, 04--民革党员,05--民盟盟员,06--民建会员,07--民进会员
 *            08--农工党党员,09--致公党党员,10--九三学社社员,11--台盟盟员,12--无党派人士,13--群众
 *   source_type--实名制来源,
 *          1 实名制客户端，2 中孚 3开发区 4 工汇 99 未知
 * 	 job_date--入职时间,
 * 	 current_city_code--当前所在城市,
 * 	 current_county_code--当前所在区域编码,
 * 	 current_county_name--当前所在区域名称,
 *   create_time--数据创建时间,
 * 	 worktype_no--当前所做工种的编码,
 * 	 worktype_name--当前所做工种的名称,
 *   is_job--就职状态(1--在职,0--离职),
 *   real_amt--欠薪资金金额,
 *   arrears_status--欠薪状态(1--欠薪,0--未欠薪)
 *   是否是特种工:
 *
 * @author linzhy
 */
object LabourerCDMV2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()

    //读取表数据注册临时表
    queryDataCreateView(spark,config)

    val sql=
      """
        |SELECT
        |	GL.lid,
        | GL.nation_id,
        | GL.real_name,
        | GL.id_card_no,
        | GL.gender,
        | DATE_FORMAT(GL.birthday,'Y-M-d') birthday,
        | YEAR(now()) - YEAR(GL.birthday) as age,
        | GL.married,
        | GL.real_grade,
        | GL.province_code,
        | GL.city_code,
        | GL.county_code,
        | GL.culture_level_type,
        | GL.politics_type,
        | GL.source_type,
        |	CL.job_date,
        |	CL.current_city_code,
        |	CL.current_county_code,
        |	CL.current_county_name,
        | CL.area_county_name,
        | CL.create_time,
        |	CL.worktype_no,
        |	CL.worktype_name,
        | CASE WHEN CL.top IS NULL THEN 1 else CL.top END top,
        | CASE WHEN CL.is_deleted IS NULL THEN 1 else CL.is_deleted END is_deleted,
        | case when CL.is_job is null or CL.is_job=false then 0 else 1 end as is_job,
        | mal.real_amt,
        | CASE WHEN mal.lid is null THEN 0 ELSE 1 END AS arrears_status
        |FROM (
        | select
        |   *
        | from
        |	  gx_labourer GL where GL.is_deleted=1
        | ) GL
        |LEFT JOIN (
        |		SELECT
        |			gcl.lid,
        |			gcl.real_name,
        |			gcl.job_date,
        |			gcl.current_city_code,
        |			gcl.current_county_code,
        |			gcl.current_county_name,
        |     AREA.name area_county_name,
        |			gcl.create_time,
        |			gcl.worktype_no,
        |			gcl.worktype_name,
        |			gcl.is_job,
        |			row_number() over( PARTITION BY gcl.lid ORDER BY gcl.is_job DESC,gcl.is_deleted ASC, gcl.create_time ASC nulls last ) top,
        |     gcl.is_deleted
        |		FROM
        |			gx_company_labourer gcl LEFT JOIN aj_dict_area AREA ON gcl.current_county_code=AREA.code
        |     AND AREA.LEVEL = 3
        |) CL ON GL.lid = CL.lid
        |LEFT JOIN  (
        | select
        |   mal.lid,
        |   SUM ( mal.real_amt ) real_amt
        | from
        |   gx_mig_arrears_labour mal
        | where  mal.is_deleted= 1
        | GROUP BY lid
        | ) mal ON GL.lid=mal.lid
        |
        |""".stripMargin


    val dataFrame = spark.sql(sql)

    PgSqlUtil.saveDataToPgsqlCdm(SaveMode.Overwrite,dataFrame,config,"labourer_day_V2")

   // dataFrame.write.format("delta").mode("overwrite").save("outdata/delta_labourer_day")
    spark.stop()

  }

  //读取数据库数据
  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    //项目民工表
    val gx_company_labourer = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_company_labourer")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //劳务人员
    val gx_labourer = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_labourer")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //欠薪员工表
    val gx_mig_arrears_labour = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_mig_arrears_labour")
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

    gx_labourer.createOrReplaceTempView("gx_labourer")
    gx_company_labourer.createOrReplaceTempView("gx_company_labourer")
    gx_mig_arrears_labour.createOrReplaceTempView("gx_mig_arrears_labour")
    aj_dict_area.createOrReplaceTempView("aj_dict_area")

  }
}

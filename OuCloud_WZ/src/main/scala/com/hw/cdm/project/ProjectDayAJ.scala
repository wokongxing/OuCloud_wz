package main.scala.com.hw.cdm.project

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**项目维度指标 数据聚合 清洗 -每天凌晨运行
 * 数据来源:
 *    aj_projects-项目信息表;
 *    gx_company_labourer--企业员工关系表,gx_labourer--劳务人员表;
 *    gx_mig_special_account--专项账户表,bank_account--银行账户表;
 *    bank_safeguard--企业保障金表;
 *    project_billboard--项目公示牌;
 *    project_attendance_ext-项目考勤表;
 *    contract--项目合同表;
 *    project_tripartite_agreement--三方监管协议
 * 获取字段:
 *    项目id,企业id,项目类型-(1-房屋;2-市政;3-附属;99-其他),
 *    project_status-项目状态(1-正常,2-停工,3-竣工),
 *    项目省市区,项目创建时间(年月日),
 *    is_realname-项目已实名(1-是,0-否),
 *    project_account_status-是否有专项账户(1-是,0-否),
 *    safeguard_status-已缴纳保障金(1-是,0-否)
 *    assure_amt --保证金金额(单位万)
 *    billboards-项目公示牌数,
 * 	  is_attendance-项目是否云考勤(1-是,0-否),
 *    contract_status-项目是否签订劳务合同(1-是,0-否),
 * 	  is_agreement-项目是否有三方监管协议(1-是,0-否)
 *
 * @author lzhy
 */
object ProjectDayAJ {
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
        |	P.pid,
        |	P.cid,
        |	P.project_type,
        |CASE
        |		WHEN P.is_completed = 'Y' THEN 3
        |		WHEN P.is_lock = 'Y' THEN 2
        |   ELSE 1
        |	END AS project_status,
        |	P.province_code,
        |	P.city_code,
        |	P.county_code,
        |	area.NAME province_name,
        |	area2.NAME city_name,
        |	area3.NAME county_name,
        | date_format(p.create_time,'Y-M-d') create_time,
        |	YEAR ( P.create_time ) year,
        |	MONTH ( P.create_time ) month,
        |	DAY ( P.create_time ) day,
        |	IFNULL( prn.is_realname,0 ) is_realname,
        |	IFNULL( pro_bank.project_account_status,0 ) project_account_status,
        | CASE WHEN mcs.cid IS NULL THEN 0 ELSE 1 END AS safeguard_status,
        | IFNULL( mcs.assure_amt,0 ) assure_amt,
        |	IFNULL( pro_bill.billboards,0 ) billboards,
        |	IFNULL( pae.is_attendance,0 ) is_attendance,
        |	IFNULL( pro_con.contract_status,0 ) contract_status,
        |	CASE WHEN pta.pid IS NULL THEN 0 ELSE 1 END AS is_agreement
        |FROM
        |	aj_projects P
        |	LEFT JOIN aj_dict_area area ON area.LEVEL = 1
        |	  AND P.province_code = area.code
        |	LEFT JOIN aj_dict_area area2 ON area2.LEVEL = 2
        |	  AND P.city_code = area2.code
        |	LEFT JOIN aj_dict_area area3 ON area3.LEVEL = 3
        |	  AND P.county_code = area3.code
        |	LEFT JOIN (
        |	SELECT DISTINCT
        |		cl.source_pid,
        |		1 AS is_realname
        |	FROM
        |		gx_company_labourer cl
        |		JOIN gx_labourer lab ON cl.lid = lab.lid
        |		AND lab.real_grade != 99
        |	) prn ON P.pid = prn.source_pid
        |	LEFT JOIN (
        |	SELECT
        |		ba.pid,
        |		1 AS project_account_status
        |	FROM
        |		gx_mig_special_account msa
        |		JOIN gx_bank_account ba ON msa.is_deleted = 1
        |		  AND msa.status = 1
        |		  AND msa.account_no = ba.bank_account_no
        |		  AND msa.cid = ba.cid
        |		  AND ba.pid != 0
        |	) pro_bank ON P.pid = pro_bank.pid
        | LEFT JOIN gx_mig_company_safeguard mcs ON mcs.is_deleted=1
        |   AND p.cid = mcs.cid
        |   AND (mcs.status=3 OR mcs.status=4)
        |	LEFT JOIN (
        |	SELECT
        |		pb.pid,
        |		COUNT ( pb.pid ) billboards
        |	FROM
        |		gx_project_billboard pb
        |	WHERE
        |		pb.is_deleted = 1
        |	GROUP BY
        |		pb.pid
        |	) pro_bill ON P.pid = pro_bill.pid
        |	LEFT JOIN gx_project_attendance_ext pae ON P.pid = pae.pid
        |	  AND pae.is_deleted = 1
        |	LEFT JOIN (
        |	SELECT
        |		co.pid,
        |		1 AS contract_status
        |	FROM
        |		gx_contract co
        |	WHERE
        |		co.pid IS NOT NULL
        |		AND co.is_deleted = 1
        |		AND co.is_stop = FALSE
        |	GROUP BY
        |		co.pid
        |	) pro_con ON P.pid = pro_con.pid
        |	LEFT JOIN gx_project_tripartite_agreement pta ON P.pid = pta.pid
        |""".stripMargin


    val dataFrame = spark.sql(sql)
    PgSqlUtil.saveDataToPgsqlCdm(SaveMode.Overwrite,dataFrame,config,"project_day")

    //spark.sql(sql).show(100)
    spark.stop();
  }

  //读取数据库数据
  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val ods_url = config.getString("pg.oucloud_ods.url")
    val ods_user = config.getString("pg.oucloud_ods.user")
    val ods_password = config.getString("pg.oucloud_ods.password")

    //获取项目信息
    val projectdf = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "aj_projects")
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
    //获取 项目员工表
    val gx_company_labourer = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_company_labourer")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //劳务工人表
    val gx_labourer = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_labourer")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //专项账户表
    val gx_mig_special_account = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_mig_special_account")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //银行账户表
    val gx_bank_account = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_bank_account")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //企业保障金
    val gx_mig_company_safeguard = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_mig_company_safeguard")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    //项目公示牌
    val gx_project_billboard = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_project_billboard")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    val gx_project_attendance_ext = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_project_attendance_ext")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    val gx_contract = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_contract")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    val gx_project_tripartite_agreement = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_project_tripartite_agreement")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    projectdf.createOrReplaceTempView("aj_projects")
    aj_dict_area.createOrReplaceTempView("aj_dict_area")
    gx_company_labourer.createOrReplaceTempView("gx_company_labourer")
    gx_labourer.createOrReplaceTempView("gx_labourer")
    gx_mig_special_account.createOrReplaceTempView("gx_mig_special_account")
    gx_mig_company_safeguard.createOrReplaceTempView("gx_mig_company_safeguard")
    gx_bank_account.createOrReplaceTempView("gx_bank_account")
    gx_project_billboard.createOrReplaceTempView("gx_project_billboard")
    gx_project_attendance_ext.createOrReplaceTempView("gx_project_attendance_ext")
    gx_contract.createOrReplaceTempView("gx_contract")
    gx_project_tripartite_agreement.createOrReplaceTempView("gx_project_tripartite_agreement")
  }
}

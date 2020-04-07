package main.scala.com.hw.cdm.project

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**项目维度指标 数据聚合 清洗 -每天凌晨运行
 * 数据来源:
 *    gx_project-项目信息表;
 *    gx_company_labourer--企业员工关系表,
 *    gx_mig_special_account--专项账户表,
 *    bank_safeguard--企业保障金表;
 *    project_billboard--项目公示牌;
 *    project_attendance_ext-项目考勤表;
 *    gx_audit--审批表;
 *    project_attachment--项目劳务合同附件表;
 *    project_tripartite_agreement--三方监管协议;
 *    gx_salary_batch -- 发薪表
 * 获取字段:
 *    项目id,企业id,项目类型-(1-房屋;2-市政;3-附属;99-其他),
 *    status 项目状态 1：已保存（审核未通过） 2：提交（未审核） 3：待分配（审核通过）4：进行中（已分配）5：完结,
 *    is_deleted--是否被删除 1 --否; 2--是,
 *    response_code -- 项目代码,
 * 	  build_area -- 项目建设面积,
 *    construct_cost --项目投资总额,
 *    create_user --项目创建人,
 *    project_condition-项目状态(1-正常,2-停工,3-竣工),
 *    项目省市区,项目创建时间(年月日),
 *    is_realname-项目已实名(1-是,0-否),
 *    project_account_status-是否有专项账户(1-是,0-否),
 *    safeguard_status-已缴纳保障金(1-是,0-否)
 *    assure_amt --保证金金额(单位万)
 *    billboards-项目公示牌数,
 * 	  is_attendance-项目是否云考勤(1-是,0-否),
 *    contract_status-项目是否签订劳务合同(1-是,0-否),
 * 	  is_agreement-项目是否有三方监管协议(1-是,0-否)
 *    real_amt -- 项目所发薪资
 *
 * @author lzhy
 */
object ProjectDayGX {
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
        |	P.response_code,
        |	P.build_area,
        | p.construct_cost,
        |	P.project_type,
        | P.status,
        | p.is_deleted,
        | p.project_condition,
        | p.create_user,
        |	P.province_code,
        |	P.city_code,
        |	P.county_code,
        |	area.NAME province_name,
        |	area2.NAME city_name,
        |	area3.NAME county_name,
        | date_format(p.create_time,'YYYY-MM-dd') create_time,
        |	YEAR ( P.create_time ) year,
        |	MONTH ( P.create_time ) month,
        |	DAY ( P.create_time ) day,
        |	IFNULL( prn.is_realname,0 ) is_realname,
        |	IFNULL( pro_bank.project_account_status,0 ) project_account_status,
        | mcs.safeguard_status,
        | IFNULL( mcs.assure_amt,0 ) assure_amt,
        |	IFNULL( pro_bill.billboards,0 ) billboards,
        |	IFNULL( pe.is_attendance,0 ) is_attendance,
        |	IFNULL( pro_con.contract_status,0 ) contract_status,
        |	IFNULL( pta.is_agreement,0 ) is_agreement,
        | sb.real_amt
        |FROM
        |	gx_project P
        |	LEFT JOIN aj_dict_area area ON area.LEVEL = 1
        |	  AND P.province_code = area.code
        |	LEFT JOIN aj_dict_area area2 ON area2.LEVEL = 2
        |	  AND P.city_code = area2.code
        |	LEFT JOIN aj_dict_area area3 ON area3.LEVEL = 3
        |	  AND P.county_code = area3.code
        |	LEFT JOIN (
        |	  SELECT
        |	    cl.source_pid,
        |	    1 AS is_realname
        |   FROM
        |	    gx_company_labourer cl
        |   WHERE cl.is_job=1
        |   and cl.is_deleted=1
        |   group by cl.source_pid
        |	) prn ON P.pid = prn.source_pid
        |	LEFT JOIN (
        |	  SELECT
        |	    msa.cid ,
        |	    1 AS project_account_status
        |   FROM
        |   	gx_mig_special_account msa
        |   where  msa.is_deleted = 1
        |   AND msa.status = 1
        |   group by msa.cid
        |	) pro_bank ON P.cid = pro_bank.cid
        | LEFT JOIN (
        | 	 select
        |       mcs.cid ,
        |       1 as safeguard_status,
        |       sum( mcs.assure_amt ) assure_amt
        |    from
        |      gx_mig_company_safeguard  mcs
        |    where mcs.is_deleted=1
        |    AND (mcs.status=3 OR mcs.status=4) group by mcs.cid
        | ) mcs ON  p.cid = mcs.cid
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
        |	LEFT JOIN (
        |   SELECT
        |	     pe.pid,
        |	     1 AS is_attendance
        |   FROM
        |	    gx_project_equipment pe
        |   WHERE isdelete='N'
        |   and is_show=1
        |   GROUP BY pe.pid
        | ) pe ON P.pid = pe.pid
        |	LEFT JOIN (
        |	 select
        |	    pid,
        |	    1 as contract_status
        | from
        |	  gx_project_attachment gpa join gx_audit ga on gpa.pid = ga.bus_id
        |   and ga.is_deleted=1
        |   and ga.status=1
        |   and ga.type=4
        |   where  gpa.is_deleted = 1
        |   and (gpa.contract1!='' or gpa.contract2!='' or gpa.contract3 !='' or gpa.contract4 !='' or gpa.contract5 !='')
        |   group by gpa.pid
        |	) pro_con ON P.pid = pro_con.pid
        |	LEFT JOIN (
        |   select
        |	    pae.pid,
        |	    1 as is_agreement
        |   from
        |	    gx_project_tripartite_agreement pae join gx_audit ga on pae.pid = ga.bus_id
        |   and ga.is_deleted=1
        |   and ga.status=1
        |   and ga.type=5
        |   where  pae.is_deleted = 1
        |   group by pae.pid
        |  ) pta ON P.pid = pta.pid
        | LEFT JOIN (
        |   SELECT
        |	    SUM(sb.real_amt) real_amt,
        |	    sb.source_pid
        |   FROM
        |	    gx_salary_batch sb
        |   WHERE sb.status = 1
        |	  AND sb.TYPE =1
        |	  GROUP BY source_pid
        | )  sb ON P.pid = sb.source_pid
        |""".stripMargin


    val dataFrame = spark.sql(sql)
    PgSqlUtil.saveDataToPgsqlCdm(SaveMode.Overwrite,dataFrame,config,"project_day")

//    spark.sql(sql).show(100)
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
      .option("dbtable", "gx_project")
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

    //专项账户表
    val gx_mig_special_account = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_mig_special_account")
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

    //薪资发放主表
    val gx_salary_batch = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable", "gx_salary_batch")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()

    //项目是否考勤
    val gx_project_equipment = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_project_equipment")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //项目合同
    val gx_project_attachment = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_project_attachment")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //三方协议
    val gx_audit = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_audit")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()
    //审批表
    val gx_project_tripartite_agreement = spark.read.format("jdbc")
      .option("url", ods_url)
      .option("dbtable","gx_project_tripartite_agreement")
      .option("user", ods_user)
      .option("password", ods_password)
      .load()


    projectdf.createOrReplaceTempView("gx_project")
    aj_dict_area.createOrReplaceTempView("aj_dict_area")
    gx_company_labourer.createOrReplaceTempView("gx_company_labourer")
    gx_mig_special_account.createOrReplaceTempView("gx_mig_special_account")
    gx_mig_company_safeguard.createOrReplaceTempView("gx_mig_company_safeguard")
    gx_project_billboard.createOrReplaceTempView("gx_project_billboard")
    gx_project_equipment.createOrReplaceTempView("gx_project_equipment")
    gx_project_attachment.createOrReplaceTempView("gx_project_attachment")
    gx_project_tripartite_agreement.createOrReplaceTempView("gx_project_tripartite_agreement")
    gx_salary_batch.createOrReplaceTempView("gx_salary_batch")
    gx_audit.createOrReplaceTempView("gx_audit")

  }
}

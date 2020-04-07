package hw.test

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object ProjectNormFulfilStatisticsTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields", "100")
      .getOrCreate()

    var config = ConfigFactory.load()

    val sps_url = config.getString("db.csb.url")
    val sps_user = config.getString("db.csb.user")
    val sps_password = config.getString("db.csb.password")

    val ods_qx_project = spark.read.format("jdbc")
      .option("url", sps_url)
      .option("dbtable", "project")
      .option("user", sps_user)
      .option("password", sps_password)
      .load()
    val ods_qx_mig_company_safeguard = spark.read.format("jdbc")
      .option("url", sps_url)
      .option("dbtable", "mig_company_safeguard")
      .option("user", sps_user)
      .option("password", sps_password)
      .load()
    val ods_qx_project_billboard = spark.read.format("jdbc")
      .option("url", sps_url)
      .option("dbtable", "project_billboard")
      .option("user", sps_user)
      .option("password", sps_password)
      .load()
    val ods_qx_mig_labourer_black = spark.read.format("jdbc")
      .option("url", sps_url)
      .option("dbtable", "mig_labourer_black")
      .option("user", sps_user)
      .option("password", sps_password)
      .load()
    val ods_qx_company = spark.read.format("jdbc")
      .option("url", sps_url)
      .option("dbtable", "company")
      .option("user", sps_user)
      .option("password", sps_password)
      .load()
    ods_qx_project.createOrReplaceTempView("ods_qx_project")
    ods_qx_mig_company_safeguard.createOrReplaceTempView("ods_qx_mig_company_safeguard")
    ods_qx_project_billboard.createOrReplaceTempView("ods_qx_project_billboard")
    ods_qx_mig_labourer_black.createOrReplaceTempView("ods_qx_mig_labourer_black")
    ods_qx_company.createOrReplaceTempView("ods_qx_company")

    val sqlstr=
      """
        |select
        |	concat(qp.province_code,qp.city_code) as id,
        |	count(*) project_count,
        |	sum (case when project_condition=3 then 1 else 0 end) as project_construction,
        |	sum (case when project_condition=5 then 1 else 0 end ) as project_stop,
        |	sum (case when project_condition=6 then 1 else 0 end ) as project_completed,
        |	count(DISTINCT qc.cid) company_count,
        |	cs.company_bond_count ,
        |	sum(bp.billboard_count) billboard_count,
        |	qp.province_code,
        |	qp.city_code,
        |	'' as create_user,
        |	now() as create_time,
        |	'' as last_modify_user,
        |	now() as last_modify_time,
        |	1 as is_deleted
        |from
        |	(select pid,cid,project_condition,province_code,city_code from ods_qx_project where is_deleted=1) qp
        |LEFT JOIN (
        |   select
        |      cid,
        |      company_name
        |   from
        |      ods_qx_company
        |   group by cid, company_name
        |) qc
        |ON      qp.cid = qc.cid
        |left join (
        | SELECT
        |		count(DISTINCT cid ) as company_bond_count,
        |		province_code,
        |		city_code
        |	FROM
        |		ods_qx_mig_company_safeguard
        |	WHERE is_deleted = 1
        |	AND status in (3,4)
        |	group by  city_code,province_code
        |) cs on qp.province_code = cs.province_code
        |	  and qp.city_code = cs.city_code
        |LEFT JOIN (
        |    SELECT
        |	    pid,
        |	    count( * )  as billboard_count
        |   FROM
        |	    ods_qx_project_billboard
        |   WHERE is_deleted =1
        |   group by pid
        |) bp
        |ON      qp.pid = bp.pid
        |group by qp.province_code,qp.city_code,cs.company_bond_count
        |""".stripMargin

    val dataFrame = spark.sql(sqlstr)
    dataFrame.printSchema()
    dataFrame.show(1001)
//
//    val tableSchema = dataFrame.schema
//    val columns =tableSchema.fields.map(x => x.name).mkString(",")
//
//    val update = tableSchema.fields.map(x =>
//      x.name.toString + "=?"
//    ).mkString(",")
//
//    println(update)

    spark.stop()

  }

}

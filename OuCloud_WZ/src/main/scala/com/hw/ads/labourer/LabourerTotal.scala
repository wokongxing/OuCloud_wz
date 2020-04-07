package main.scala.com.hw.ads.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 数据来源:
 *       CDM层 labourer_day_v2 --每一个民工离职 在职情况;
 * 获取字段:
 *    劳务员工总人数:--labourer_total,
 *    在职民工人数:--is_job_total, --注:数据中可能存在 is_job=1 and is_deleted=2的情况 即统计时 需加上 is_deleted状态
 *    离职民工人数:--no_job_total,
 *    民工男性数量:--man_total,
 *    民工女性数量:--woman_total,
 *    年龄段分布:
 *        age_dist_one(15-20) 包含15,
 *        age_dist_two(20-25) 包含20,
 *        age_dist_three(25-30) 包含25,
 *        age_dist_four(30-35) 包含30,
 *        age_dist_five(35-40) 包含35,
 *        age_dist_six(40-45) 包含40,
 *        age_dist_seven(45-50) 包含45,
 *        age_dist_eight(50以上) 包含50,
 *    特种工数量: --待统计
 *        依据 检测 安拆 工地 分别统计 占比 数量
 * @author lzhy
 */
object LabourerTotal {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()

    queryDataCreateView(spark,config)

    val sql=
      """
        |SELECT
        |	COUNT ( lid ) labourer_total,
        |	SUM ( CASE WHEN ld.is_job = 1 and ld.is_deleted=1 THEN 1 ELSE 0 END ) is_job_total,
        |	SUM ( CASE WHEN ld.is_job = 0 or  ld.is_deleted=2 THEN 1 ELSE 0 END ) no_job_total,
        |	SUM ( CASE WHEN ld.gender = '男' THEN 1 ELSE 0 END ) man_total,
        |	SUM ( CASE WHEN ld.gender = '女' THEN 1 ELSE 0 END ) woman_total,
        |	SUM ( CASE WHEN ld.age >= 15 AND ld.age < 20 THEN 1 ELSE 0 END ) age_dist_one,
        |	SUM ( CASE WHEN ld.age >= 20 AND ld.age < 25 THEN 1 ELSE 0 END ) age_dist_two,
        |	SUM ( CASE WHEN ld.age >= 25 AND ld.age < 30 THEN 1 ELSE 0 END ) age_dist_three,
        |	SUM ( CASE WHEN ld.age >= 30 AND ld.age < 35 THEN 1 ELSE 0 END ) age_dist_four,
        |	SUM ( CASE WHEN ld.age >= 35 AND ld.age < 40 THEN 1 ELSE 0 END ) age_dist_five,
        |	SUM ( CASE WHEN ld.age >= 40 AND ld.age < 45 THEN 1 ELSE 0 END ) age_dist_six,
        |	SUM ( CASE WHEN ld.age >= 45 AND ld.age < 50 THEN 1 ELSE 0 END ) age_dist_seven,
        |	SUM ( CASE WHEN ld.age >= 50 THEN 1 ELSE 0 END ) age_dist_eight,
        | now() create_time
        |FROM
        |	labourer_day_v2 ld
        | WHERE LD.top=1
        | and ld.real_grade !=99
        |
        |""".stripMargin



    val dataFrame = spark.sql(sql)
    dataFrame.show(100)
    //PgSqlUtil.saveDataToPgsqlAds(SaveMode.Append,dataFrame,config,"labourer_total")

    spark.stop();
  }

  //读取数据库数据
  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")

    //获取劳务员工聚合信息
    val labourer_day_v2 = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "labourer_day_v2")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    labourer_day_v2.createOrReplaceTempView("labourer_day_v2")

  }
}

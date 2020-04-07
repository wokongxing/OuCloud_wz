package main.scala.com.hw.ads.labourer

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

/** 需求:
 *    统计银行排行榜
 *      示例: 以发薪金额降序,发薪人数降序,发薪项目降序排行
 *
 *  数据来源:
 *      ADS层: labourer_bank_area_month 银行信息表
 *
 *  获取字段:
 *      银行名称:--bank_name;
 *      发薪总金额:--real_amt_total,
 *      发薪人数总数:--labourer_total,
 *      发薪的项目数:--project_total;
 *      排名:--rank;
 *
 * @author linzhy
 */
object LabourerBankRank extends Logging{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                  .appName(this.getClass.getSimpleName)
                  .master("local[2]")
                  .getOrCreate()

    var config = ConfigFactory.load()
//  logWarning("你好 !,你正在运行此运用")
    queryDataCreateView(spark,config)

    //输入条件
    val input_yeartime = "2018";
    val input_county_code ="330301";

    val sql =
      s"""
        |SELECT
        |	LBA.bank_name,
        |	LBA.real_amt,
        |	LBA.real_number,
        |	LBA.project_total,
        | ROW_NUMBER() OVER( ORDER BY LBA.real_amt DESC,LBA.real_number DESC ,LBA.project_total DESC) RANK
        |FROM
        |(
        | SELECT
        |	  bank_name,
        |	  SUM ( real_amt ) real_amt,
        |	  SUM ( real_number ) real_number,
        |	  SUM ( project_total ) project_total
        | FROM
        |	  labourer_bank_area_month
        | WHERE year='$input_yeartime'
        |   AND county_code='$input_county_code'
        | GROUP BY
        |	  bank_name
        |) LBA
        |""".stripMargin

    val dataFrame = spark.sql(sql)
    val tableSchema = dataFrame.schema
//    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Append,dataFrame,config,"labourer_bank_rank")


    //dataFrame.show(1000)

    spark.stop()

  }

  def queryDataCreateView(spark: SparkSession, config:Config): Unit ={

    val ads_url = config.getString("pg.oucloud_ads.url")
    val ads_user = config.getString("pg.oucloud_ads.user")
    val ads_password = config.getString("pg.oucloud_ads.password")

    //Ads层
    val labourer_bank_area_month = spark.read.format("jdbc")
      .option("url", ads_url)
      .option("dbtable", "labourer_bank_area_month")
      .option("user", ads_user)
      .option("password", ads_password)
      .load()

    labourer_bank_area_month.createOrReplaceTempView("labourer_bank_area_month")
    spark.sqlContext.sql("")
  }
}

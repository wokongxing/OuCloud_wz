package  main.scala.com.hw.ads.companys

import com.typesafe.config.ConfigFactory
import main.scala.com.hw.utils.PgSqlUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 统计企业 维度指标总计
 * --数据来源: oucloud_Ads----companysday 日统计数据表
 * --获取字段:
 *        按区域划分:管辖内  业务企业(业务本地,业务外地,业务其他) 数量以及占比
 *    区域:county_name;
 *    行政企业数量: xz_companys_sum;
 *    企业类型(epc)数量: EPC_count;
 *    epc企业占比: epc_proportion;
 *    施工企业数量: company_construction_count;
 *    施工企业占比: construction_proportion;
 * 	  劳务企业数量: company_labour_count;
 * 	  劳务企业数量占比: labour_proportion;
 * 	  本地企业数量: company_local_count;
 * 	  本地企业数量占比: local_proportion;
 * 	  外地本地数量: company_nonlocal_count;
 * 	  外地企业数量占比: nonlocal_proportion;
 * 	  未知企业数量: other;
 * 	  未知企业数量占比: other_proportion;
 * 	  业务企业数量: yw_companys_sum;
 * 	  业务本地企业数量: yw_company_local_count;
 * 	  业务本地数量占比: yw_local_proportion;
 * 	  业务非本地企业数量:  yw_company_nonlocal_count;
 * 	  业务非本地数量占比:  yw_nonlocal_proportion;
 * 	  业务未知企业数量: yw_other;
 * 	  业务未知企业数量占比: yw_other_proportion;
 * @author linzhy
 */
object CompanysTotalV2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer") //序列化
      .getOrCreate()

    var config = ConfigFactory.load()
    val ads_url = config.getString("pg.oucloud_ads.url")
    val ads_user = config.getString("pg.oucloud_ads.user")
    val ads_password = config.getString("pg.oucloud_ads.password")

    //读取comapanysday表 信息
    val ads_companys_day = spark.read.format("jdbc")
      .option("url", ads_url)
      .option("dbtable","ads_companys_day")
      .option("user", ads_user)
      .option("password", ads_password)
      .load()

    //根据天统计量,统计总指标
    ads_companys_day.createOrReplaceTempView("ads_companys_day")

    val sql=
      """

        |SELECT
        | concat(county_code,date_format(now(),'YYYYMMdd')) pkid,
        |	coms.county_name,
        |	coms.xz_companys_sum,
        | coms.epc_count,
        | concat(round(coms.EPC_count/coms.xz_companys_sum * 100,2),'%') epc_proportion,
        |	coms.company_construction_count,
        |	concat ( round( coms.company_construction_count / coms.xz_companys_sum * 100, 2 ), '%' ) construction_proportion,
        |	coms.company_labour_count,
        |	concat ( round( coms.company_labour_count / coms.xz_companys_sum * 100, 2 ), '%' ) labour_proportion,
        |	coms.xz_local_count,
        |	concat ( round( coms.xz_local_count / coms.xz_companys_sum * 100, 2 ), '%' ) xz_local_proportion,
        |	coms.xz_nonlocal_count,
        |	concat ( round( coms.xz_nonlocal_count / coms.xz_companys_sum * 100, 2 ), '%' ) xz_nonlocal_proportion,
        |	coms.xz_other,
        |	concat ( round( coms.xz_other / coms.xz_companys_sum * 100, 2 ), '%' ) xz_other_proportion,
        |	coms.yw_companys_sum,
        |	coms.yw_local_count,
        |	concat ( round( coms.yw_local_count / coms.yw_companys_sum * 100, 2 ), '%' ) yw_local_proportion,
        |	coms.yw_nonlocal_count,
        |	concat ( round( coms.yw_nonlocal_count / coms.yw_companys_sum * 100, 2 ), '%' ) yw_nonlocal_proportion,
        |	coms.yw_other,
        |	concat ( round( coms.yw_other / coms.yw_companys_sum * 100, 2 ), '%' ) yw_other_proportion,
        | now() create_time
        |FROM
        |	(
        |	SELECT
        |   d.county_code,
        |		d.county_name,
        |		SUM ( d.xz_companys_sum ) xz_companys_sum,
        |   SUM ( d.EPC_count) EPC_count,
        |		SUM ( d.company_construction_count ) company_construction_count,
        |		SUM ( d.company_labour_count ) company_labour_count,
        |		SUM ( d.xz_local_count ) xz_local_count,
        |		SUM ( d.xz_nonlocal_count ) xz_nonlocal_count,
        |		SUM ( d.xz_other ) xz_other,
        |		SUM ( d.yw_companys_sum ) yw_companys_sum,
        |		SUM ( d.yw_local_count ) yw_local_count,
        |		SUM ( d.yw_nonlocal_count ) yw_nonlocal_count,
        |		SUM ( d.yw_other ) yw_other
        |	FROM
        |		ads_companys_day d
        |	GROUP BY
        |	county_name,county_code
        |	) coms order by county_code desc
        |""".stripMargin

    val dataFrame = spark.sql(sql)
//    PgSqlUtil.saveDataToPgsqlAds(SaveMode.Overwrite,dataFrame,config,"ads_companys_total")
    dataFrame.printSchema()
    val conn = PgSqlUtil.connectionPool("OuCloud_ADS")
    PgSqlUtil.insertOrUpdateToPgsql(conn,dataFrame,spark.sparkContext,"ads_companys_total","pkid")
    spark.stop()
  }

}

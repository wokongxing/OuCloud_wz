package hw.test

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType}

import scala.reflect.internal.util.TableDef.Column

/**

 *
 * @author linzhy
 */
object GetJson {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config = ConfigFactory.load()
    val cdm_url = config.getString("db.sptn.url")
    val cdm_user = config.getString("db.sptn.user")
    val cdm_password = config.getString("db.sptn.password")

    //项目民工表
    val sys_openapi_log = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "sys_openapi_log")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()
    val request_audit_logs = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "request_audit_logs")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()
    sys_openapi_log.createOrReplaceTempView("sys_openapi_log")
    request_audit_logs.createOrReplaceTempView("request_audit_logs")

    val sql1=
      """
        |
        |SELECT
        |    GET_JSON_OBJECT(request_content,'$.dto.method') method
        |    ,GET_JSON_OBJECT(request_content,'$.dto.appid') appid
        |    ,GET_JSON_OBJECT(request_content,'$.dto.data') data
        |    ,GET_JSON_OBJECT(response_content,'$.code') code
        |    ,GET_JSON_OBJECT(response_content,'$.message') message
        |FROM    request_audit_logs
        |WHERE   api = '/openapi'
        |AND     GET_JSON_OBJECT(response_content,'$.code') != 0
        |and GET_JSON_OBJECT(request_content,'$.dto.method')='labourer.import'
        |and GET_JSON_OBJECT(request_content,'$.dto.appid')=1001
        |union
        |SELECT
        |    GET_JSON_OBJECT(request_content,'$.dto.method') method
        |   ,GET_JSON_OBJECT(request_content,'$.dto.appid') appid
        |   ,GET_JSON_OBJECT(request_content,'$.dto.data') data
        |   ,GET_JSON_OBJECT(response_content,'$.code') code
        |   ,GET_JSON_OBJECT(response_content,'$.message') message
        |FROM    ods_gx_request_audit_logs_20190729
        |WHERE   api = '/openapi'
        |AND     GET_JSON_OBJECT(response_content,'$.code') != 0
        |and GET_JSON_OBJECT(request_content,'$.dto.method')='labourer.import'
        |and GET_JSON_OBJECT(request_content,'$.dto.appid')='1001'
        |union
        |SELECT
        |    GET_JSON_OBJECT(request_content,'$.dto.method') method
        |   ,GET_JSON_OBJECT(request_content,'$.dto.appid') appid
        |   ,GET_JSON_OBJECT(request_content,'$.dto.data') data
        |   ,GET_JSON_OBJECT(response_content,'$.code') code
        |   ,GET_JSON_OBJECT(response_content,'$.message') message
        |FROM    ods_gx_request_audit_logs_20191107
        |WHERE   api = '/openapi'
        |AND     GET_JSON_OBJECT(response_content,'$.code') != 0
        |and GET_JSON_OBJECT(request_content,'$.dto.method')='labourer.import'
        |and GET_JSON_OBJECT(request_content,'$.dto.appid')='1001'
        |union
        |SELECT
        |    GET_JSON_OBJECT(request_content,'$.dto.method') method
        |   ,GET_JSON_OBJECT(request_content,'$.dto.appid') appid
        |   ,GET_JSON_OBJECT(request_content,'$.dto.data') data
        |   ,GET_JSON_OBJECT(response_content,'$.code') code
        |   ,GET_JSON_OBJECT(response_content,'$.message') message
        |FROM    ods_gx_request_audit_logs_20200115
        |WHERE   api = '/openapi'
        |AND     GET_JSON_OBJECT(response_content,'$.code') != 0
        |and GET_JSON_OBJECT(request_content,'$.dto.method')='labourer.import'
        |and GET_JSON_OBJECT(request_content,'$.dto.appid')='1001'
        |union
        |SELECT
        |    GET_JSON_OBJECT(request_content,'$.dto.method') method
        |   ,GET_JSON_OBJECT(request_content,'$.dto.appid') appid
        |   ,GET_JSON_OBJECT(request_content,'$.dto.data') data
        |   ,GET_JSON_OBJECT(response_content,'$.code') code
        |   ,GET_JSON_OBJECT(response_content,'$.message') message
        |FROM    ods_gx_request_audit_logs_20200301
        |WHERE   api = '/openapi'
        |AND     GET_JSON_OBJECT(response_content,'$.code') != 0
        |and GET_JSON_OBJECT(request_content,'$.dto.method')='labourer.import'
        |and GET_JSON_OBJECT(request_content,'$.dto.appid')='1001'
        |union
        |SELECT
        |    GET_JSON_OBJECT(request_content,'$.dto.method') method
        |   ,GET_JSON_OBJECT(request_content,'$.dto.appid') appid
        |   ,GET_JSON_OBJECT(request_content,'$.dto.data') data
        |   ,GET_JSON_OBJECT(response_content,'$.code') code
        |   ,GET_JSON_OBJECT(response_content,'$.message') message
        |FROM    ods_gx_request_audit_logs_20200320
        |WHERE   api = '/openapi'
        |AND     GET_JSON_OBJECT(response_content,'$.code') != 0
        |and GET_JSON_OBJECT(request_content,'$.dto.method')='labourer.import'
        |and GET_JSON_OBJECT(request_content,'$.dto.appid')='1001'
        |
        |""".stripMargin
val str="[{\\\"real_name\\\":\\\"龚声松\\\",\\\"user_photo\\\":null,\\\"gender\\\":0,\\\"birthday\\\":\\\"1994-12-12\\\",\\\"id_card_no\\\":\\\"v/MzCKoRgbyenY3L+vjW5DoqhXf4ra/c+wYYyY/xvVc=\\\",\\\"start_date\\\":null,\\\"expiry_date\\\":null,\\\"mobilephone\\\":\\\"15825641896\\\",\\\"province_code\\\":null,\\\"city_code\\\":null,\\\"county_code\\\":null,\\\"address\\\":null,\\\"current_address\\\":null,\\\"nation\\\":\\\"1\\\",\\\"married\\\":\\\"01\\\",\\\"bank_code\\\":null,\\\"bank_no\\\":null,\\\"job_date\\\":\\\"2019-05-18\\\",\\\"worktype_code\\\":\\\"1000\\\",\\\"group_name\\\":\\\"粉刷工班组\\\",\\\"project_name\\\":\\\"平阳县昆阳镇城东新区B77地块标段地块Ⅰ、Ⅱ标段\\\"}]"
    val sql2=
      """
        | select
        | regexp_replace(substr('[{\"real_name\":\"龚声松\",\"user_photo\":\"null\"}]',2,15),"\\\\","") ,
        | get_json_object(regexp_replace(substr('[{\"real_name\":\"龚声松\",\"user_photo\":\"null\"}]',2,length('[{\"real_name\":\"龚声松\",\"user_photo\":\"null\"}]')-1),"\\\\",""),'$.real_name') real_name
        |""".stripMargin

    val resultDS = spark.sql(sql2)
//    val dataFrame = resultDS.withColumn("array_str",from_json("arrary_str", ArrayType(StringType, containsNull = false))))
    resultDS.show(10)
    resultDS.printSchema()
//    dataFrame.show(100)

    spark.stop();
  }
}

package hw.ads.temp

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --项目信息总统计
 *  --数据来源:
 *     ods_gx_sys_openapi_log;
 *
 * @author linzhy
 */
object SysOpenapiLog extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |
        |insert overwrite table cdm_sys_openapi_log_detail
        |SELECT
        |   get_json_object(substr(data, 2, length(data)-1),"$.real_name") real_name,
        |   get_json_object(substr(data, 2, length(data)-1),"$.id_card_no") id_card_no,
        |   ifnull(get_json_object(substr(data, 2, length(data)-1),"$.proj.pid"),get_json_object(substr(data, 2, length(data)-1),"$.source_project.pid")) pid,
        |   ifnull(get_json_object(substr(data, 2, length(data)-1),"$.proj.project_name"),get_json_object(substr(data, 2, length(data)-1),"$.source_project.project_name")) project_name,
        |   ifnull(get_json_object(substr(data, 2, length(data)-1),"$.proj.response_code"),get_json_object(substr(data, 2, length(data)-1),"$.source_project.response_code")) response_code,
        |   create_time
        |from
        |  ods_gx_sys_openapi_log
        |where method='labourer.import'
        |  and appid=1001
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

package hw.ads.temp

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --项目信息总统计
 *  --数据来源:
 *    cdm_gx_request_audit_logs;
 *
 * @author linzhy
 */
object RequestAuditLogsTotal extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |insert overwrite table  ads_gx_request_audit_logs
        |SELECT
        |   regexp_replace(substr(data, 2, length(data)-1),"\\\\","") datas,
        |   get_json_object(regexp_replace(substr(data, 2, length(data)-1),"\\\\",""),"$.real_name") real_name,
        |   get_json_object(regexp_replace(substr(data, 2, length(data)-1),"\\\\",""),"$.mobilephone") mobilephone,
        |   get_json_object(regexp_replace(substr(data, 2, length(data)-1),"\\\\",""),"$.project_name") project_name,
        |   message,
        |   request_time
        |from
        |  cdm_gx_request_audit_logs
        |
        |""".stripMargin

    try{
      import spark._
      sql(sqlstr).show(100)
    }finally {
      spark.stop()
    }

  }

}

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
object SysOpenapiLogTotal extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |WITH LOG AS(
        |SELECT
        |   get_json_object(substr(data, 2, length(data)-1),"$.id_card_no") id_card_no,
        |   get_json_object(substr(data, 2, length(data)-1),"$.proj.pid") pid
        |from
        |  ods_gx_sys_openapi_log
        |where method='labourer.import'
        |  and appid=1001
        | )
        | SELECT
        |   id_card_no as id,
        |   COUNT(*) count,
        |   1 as type
        | FROM
        |   LOG
        | GROUP BY id_card_no
        | union
        | SELECT
        |   pid as id,
        |   COUNT(*) count,
        |   2 as type
        | FROM
        |   LOG
        | GROUP BY pid
        |
        |""".stripMargin

      val sqlstr2 =
        """
          |insert overwrite table ads_sys_openapi_log_total
          |select
          | id,
          | count,
          | type
          |from
          | temp_view
          |
          |""".stripMargin
    try{
      import spark._
      sql(sqlstr).createOrReplaceTempView("temp_view")
      sql(sqlstr2)
    }finally {
      spark.stop()
    }

  }

}

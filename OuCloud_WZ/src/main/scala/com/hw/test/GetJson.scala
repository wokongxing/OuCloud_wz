package hw.test

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

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
    val cdm_url = config.getString("db.alsp.url")
    val cdm_user = config.getString("db.alsp.user")
    val cdm_password = config.getString("db.alsp.password")

    //项目民工表
    val sys_openapi_log = spark.read.format("jdbc")
      .option("url", cdm_url)
      .option("dbtable", "sys_openapi_log")
      .option("user", cdm_user)
      .option("password", cdm_password)
      .load()

    sys_openapi_log.createOrReplaceTempView("sys_openapi_log")


    val sql2=
      """ WITH LOG AS(
        |SELECT
        |   get_json_object(substr(data, 2, length(data)-1),"$.id_card_no") id_card_no,
        |   get_json_object(substr(data, 2, length(data)-1),"$.proj.pid") pid
        |from
        |  sys_openapi_log SO
        |  where method='labourer.import'
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
        |""".stripMargin

spark.sql(sql2).printSchema()
    spark.sql(sql2).show()


    spark.stop();
  }
}

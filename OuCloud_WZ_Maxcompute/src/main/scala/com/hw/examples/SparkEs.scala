package hw.examples

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

object SparkEs extends Logging{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("estest")
//      .master("local[4]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer") //序列化
      .config("spark.debug.maxToStringFields","100")
      .config("pushdown","true")
      .config("spark.es.scroll.size","1000")
      .config("es.nodes","120.55.220.173") //注:生产请用ip
      .config("es.port","9200")
      .config("es.nodes.wan.only","true") //域名访问需要为true ,默认值为false 生产去除
      .config("es.net.http.auth.user","test")
      .config("es.net.http.auth.pass","hw191118")
      .getOrCreate()

    val index = s"ousutec-attendance-debug"

    //sql
    val frame = spark.esDF(index)
      .where("record_time > to_timestamp('2020-03-10') ")


    frame.createOrReplaceTempView("attendance")
    val sql=
      """
        |select
        | concat(pid,unix_timestamp(record_time)) id,
        | pid,
        | sn,
        | direct,
        | idcardno,
        | full_name,
        | mobile,
        | tid,
        | record_time,
        | insert_time,
        | equp_name,
        | photo_path,
        | app_key
        |from
        | attendance AT where pid=154
        |""".stripMargin

    val dataFrame = spark.sql(sql)
    dataFrame.show(1000)
    //dataFrame.printSchema()
//    val conn = PgSqlUtil.connectionPool()
//   PgSqlUtil.insertOrUpdateToPgsql(conn,dataFrame,spark.sparkContext,"kq_attendances","id")

    spark.stop()
  }

def getSql (): Unit ={
  val sql2 =
    """
      |select
      | app_key,
      | direct,
      | equp_name,
      | full_name,
      | idcardno,
      | insert_time,
      | pid,
      | record_time,
      | sn,
      | tid
      |from attendance AT where date_format(AT.insert_time,'Y-M-d')='2020-03-06'
      |""".stripMargin


  val sql3 =
    """
      |select
      | full_name,
      | idcardno,
      | pid,
      | sn,
      | record_time,
      | date_format(AT.record_time,'YYYY-MM-dd') record_time,
      | date_format(AT.record_time,'Y-MM') record_time2
      |from attendance AT where   date_format(AT.record_time,'YYYY-MM') = '2019-07' and pid='99216'
      |AND (sn = 'D10875941' or sn = 'D10875969' or sn = 'D10875965')
      |order by record_time desc
      |""".stripMargin

  val sql4 =
    """
      |select
      | full_name,
      | idcardno,
      | pid,
      | sn,
      | count(*)
      |from attendance AT where   date_format(AT.record_time,'YYYY-MM') = '2019-07' and pid='99216'
      |AND (sn = 'D10875941' or sn = 'D10875969' or sn = 'D10875965')
      |group by sn , full_name,
      | idcardno,
      | pid
      |""".stripMargin
}
}

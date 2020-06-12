package hw.examples

import org.apache.spark.sql.SparkSession

object SparkSql {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
//      .master("local[2]")
      .appName("SparkSQL-on-MaxCompute")
      .config("spark.sql.broadcastTimeout", 20 * 60)
      .config("spark.sql.crossJoin.enabled", true)
      .config("odps.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.catalogImplementation", "odps")
      .getOrCreate()

    val project = spark.conf.get("odps.project.name")
    val tableName = spark.conf.get("spark.project.tablename1")
    val tableName2 = spark.conf.get("spark.project.tablename2")

    import spark._
    import sqlContext.implicits._
    val ptTableName = "mc_test_pt_table"
    // Drop Create
//    sql(s"DROP TABLE IF EXISTS ${tableName}")
//    sql(s"DROP TABLE IF EXISTS ${ptTableName}")
//
//    sql(s"CREATE TABLE ${tableName} (name STRING, num BIGINT)")
//    sql(s"CREATE TABLE ${ptTableName} (name STRING, num BIGINT) PARTITIONED BY (pt1 STRING, pt2 STRING)")
//
    val df = spark.sparkContext.parallelize(0 to 99, 2).map(f => {
      (s"name-$f", f)
    }).toDF("name", "num")
//
//    val ptDf = spark.sparkContext.parallelize(0 to 99, 2).map(f => {
//      (s"name-$f", f, "2018", "0601")
//    }).toDF("name", "num", "pt1", "pt2")
//
//    // 写 普通表
//    df.write.insertInto(tableName) // insertInto语义
//    df.write.mode("overwrite").insertInto(tableName) // insertOverwrite语义
//
    // 写 分区表
    // DataFrameWriter 无法指定分区写入 需要通过临时表再用SQL写入特定分区
    df.createOrReplaceTempView(s"${ptTableName}_tmp_view")
    sql(s"insert into table ${ptTableName} partition (pt1='2018', pt2='0601') select * from ${ptTableName}_tmp_view")
    sql(s"insert overwrite table ${ptTableName} partition (pt1='2018', pt2='0601') select * from ${ptTableName}_tmp_view")

//    ptDf.write.insertInto(ptTableName) // 动态分区 insertInto语义
//    ptDf.write.mode("overwrite").insertInto(ptTableName) // 动态分区 insertOverwrite语义
//
    // 读 普通表
//    val sqlstr=
//        s"""
//          |SELECT
//          |   id_card_no,
//          |   source_pid,
//          |   date_format(endtime,'YYYY-MM') endtime,
//          |   date_format(starttime,'YYYY-MM') starttime,
//          |   row_number() over(partition by source_pid,id_card_no,date_format(endtime,'YYYY-MM'),date_format(starttime,'YYYY-MM')  order by source_pid desc) rank_top
//          |FROM  ${tableName}
//          |WHERE is_deleted = 1
//          |AND source_pid IN (SELECT pid FROM ${tableName2} )
//          |
//          |""".stripMargin
//
//    val rdf = sql(sqlstr)
//    rdf.show(1000)
//    rdf.printSchema()
//
//    // 读 分区表
//    val rptdf = sql(s"select name, num, pt1, pt2 from $ptTableName where pt1 = '2018' and pt2 = '0601'")
//    println(s"rptdf count, ${rptdf.count()}")
//    rptdf.printSchema()

    spark.stop()
  }

}

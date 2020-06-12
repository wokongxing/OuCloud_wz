package hw.ads.companysalary

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging

/**
 * --维权告示牌年月
 *
 *  --数据来源:
 *      ods_base_project;
 *      ods_base_project_billboard
 * --获取字段
 * `pid` ------ '项目id',
 * `status` -------'状态：1，未落实  2，已处理  3，已落实',
 * `cur_date` -----'所属年月日期',
 * `cur_date_year` ----'所属年',
 * `cur_date_month` ----'所属月',
 * `up_time` ---------- '上传时间',
 * `create_user` -------'创建人',
 * `create_time` -------'创建时间',
 * `last_modify_user`--- '修改人',
 * `last_modify_time`---- '修改时间',
 * `is_deleted`-----------'是否删除 - 1:未删除 2:删除',
 */
object ProjectInfoBillboardtemp extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val dateMaps = spark.conf.get("spark.project.dateMaps")
    val dates = dateMaps.split(",")
    var real_sql = new StringBuffer()

    if (dates.size>1) {
      dateMaps.split(",").map(x => {
        val datetime = x
        var sqlstr =
          s"""
             |SELECT
             |   concat(qp.pid,year('${datetime}'),month('${datetime}')) id,
             |	 qp.pid,
             |	 ifnull(pb.status,1) status,
             |   trunc('${datetime}','MM')  cur_date,
             |   year('${datetime}')  cur_date_year,
             |   month('${datetime}') cur_date_month,
             |   pb.create_time up_time,
             |   '' as create_user,
             |   now() as create_time,
             |   '' as last_modify_user,
             |   now() as last_modify_time,
             |   qp.is_deleted
             |from
             |  ods_base_project qp
             |left join (
             |   SELECT
             |	    pid,
             |	    3 as status,
             |     create_time,
             |     row_number() over( partition by pid order by create_time desc ) rank_top
             |   FROM
             |	    ods_base_project_billboard
             |   WHERE is_deleted =1
             |     AND create_time <= date_format('${datetime}','yyyy-MM-dd HH:mm:ss')
             |) pb on qp.pid = pb.pid and pb.rank_top=1
             |""".stripMargin
        real_sql.append(sqlstr).append("union")
      })
    }else {
      val datetime = dateMaps
      var sqlstr =
        s"""
           |SELECT
           |   concat(qp.pid,year('${datetime}'),month('${datetime}')) id,
           |	 qp.pid,
           |	 ifnull(pb.status,1) status,
           |   trunc('${datetime}','MM')  cur_date,
           |   year('${datetime}')  cur_date_year,
           |   month('${datetime}') cur_date_month,
           |   pb.create_time up_time,
           |   '' as create_user,
           |   now() as create_time,
           |   '' as last_modify_user,
           |   now() as last_modify_time,
           |   qp.is_deleted
           |from
           |  ods_base_project qp
           |left join (
           |   SELECT
           |	    pid,
           |	    3 as status,
           |     create_time,
           |     row_number() over( partition by pid order by create_time desc ) rank_top
           |   FROM
           |	    ods_base_project_billboard
           |   WHERE is_deleted =1
           |     AND create_time <= date_format('${datetime}','yyyy-MM-dd HH:mm:ss')
           |) pb on qp.pid = pb.pid and pb.rank_top=1
           |""".stripMargin
      real_sql.append(sqlstr).append("union")
    }
    val sql2 =
      """
        |insert overwrite table ads_qx_project_info_billboard_p partition (date_year,date_month)
        |select
        | id,
        | pid,
        | status,
        | cur_date,
        | cur_date_year,
        | cur_date_month,
        | up_time,
        | create_user,
        | create_time,
        | last_modify_user,
        | last_modify_time,
        | is_deleted,
        | cur_date_year AS date_year,
        | cur_date_month AS date_month
        |from temp_project_info_billboard
        |
        |""".stripMargin

    try{
      import spark._
      val sqlstr2 = real_sql.substring(0,real_sql.length()-5)
      sql(sqlstr2).createOrReplaceTempView("temp_project_info_billboard")
      sql(sql2)
    }finally {
      spark.stop()
    }

  }

}

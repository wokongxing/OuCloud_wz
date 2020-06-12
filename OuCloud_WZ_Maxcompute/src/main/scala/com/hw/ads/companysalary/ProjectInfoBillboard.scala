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
object ProjectInfoBillboard extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)
    val dateMaps = spark.conf.get("spark.project.dateMaps")

    val sql2 =
      """
        |insert overwrite table ads_qx_project_info_billboard
        |SELECT
        |concat(qp.pid,year(pb.create_time),month(pb.create_time)) id,
        |qp.pid,
        |coalesce(pb.status,1) status,
        |trunc(pb.create_time,'MM')  cur_date,
        |year(pb.create_time)  cur_date_year,
        |month(pb.create_time) cur_date_month,
        |pb.create_time up_time,
        |'' as create_user,
        |now() as create_time,
        |'' as last_modify_user,
        |now() as last_modify_time,
        |qp.is_deleted
        |from
        |ods_base_project qp
        |   join (
        |    SELECT
        |     pid,
        |     3 as status,
        |     create_time,
        |     row_number() over( partition by pid,year(pb.create_time),month(pb.create_time) order by create_time desc ) rank_top
        |    FROM
        |    ods_base_project_billboard pb
        |  ) pb on qp.pid = pb.pid and pb.rank_top=1
        |
        |""".stripMargin

    try{
      import spark._
      sql(sql2)
    }finally {
      spark.stop()
    }

  }

}

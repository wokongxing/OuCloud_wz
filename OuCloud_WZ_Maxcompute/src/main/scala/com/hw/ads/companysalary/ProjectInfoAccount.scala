package hw.ads.companysalary

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * --分账管理-进账记录年月
 *  --数据来源:
 *      ods_base_project;
 *      ods_base_bank_transfer;
 *  --获取字段:
 * `id
 * `pid----------项目id',
 * `status-------状态：1，未落实  2，已处理  3，已落实',
 * `cur_date------所属年月日期',
 * `cur_date_year--所属年',
 * `cur_date_month--所属月',
 * `amt------------金额',
 * `create_user----创建人',
 * `create_time----创建时间',
 * `last_modify_user---修改人',
 * `last_modify_time----修改时间',
 * `is_deleted-------是否删除 - 1:未删除 2:删除'
 *
 * @author linzhy
 */
object ProjectInfoAccount extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    //获取需要统计的时间
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
             |   ifnull(pb.amt,0) amt,
             |   '' as create_user,
             |   now() as create_time,
             |   '' as last_modify_user,
             |   now() as last_modify_time,
             |   qp.is_deleted
             |from
             |  ods_base_project qp
             |left join (
             |   SELECT
             |      pid,
             |      case when SUM(amt)>0 then 3 else 1 end status,
             |	    SUM(amt) amt
             |    FROM
             |	    ods_base_bank_transfer
             |    WHERE jtype = 1
             |    AND is_deleted = 1
             |    and  is_pro_valid>0
             |    and year(trans_time)=year('${datetime}')
             |    and month(trans_time)= month('${datetime}')
             |    GROUP BY pid
             |) pb on qp.pid = pb.pid
             |""".stripMargin
        real_sql.append(sqlstr).append("union")
      })
    }else {
      val datetime = dateMaps
      var sqlstr =
        s"""
           |with
           |SELECT
           |   concat(qp.pid,year('${datetime}'),month('${datetime}')) id,
           |	 qp.pid,
           |	 ifnull(pb.status,1) status,
           |   trunc('${datetime}','MM')  cur_date,
           |   year('${datetime}')  cur_date_year,
           |   month('${datetime}') cur_date_month,
           |   ifnull(pb.amt,0) amt,
           |   '' as create_user,
           |   now() as create_time,
           |   '' as last_modify_user,
           |   now() as last_modify_time,
           |   qp.is_deleted
           |from
           |  ods_base_project qp
           |left join (
           |   SELECT
           |      pid,
           |      case when SUM(amt)>0 then 3 else 1 end status,
           |	    SUM(amt) amt
           |    FROM
           |	    ods_base_bank_transfer
           |    WHERE jtype = 1
           |    AND is_deleted = 1
           |    and  is_pro_valid>0
           |    and year(trans_time)=year('${datetime}')
           |    and month(trans_time)= month('${datetime}')
           |    GROUP BY pid
           |) pb on qp.pid = pb.pid
           |
           |""".stripMargin
      real_sql.append(sqlstr).append("union")
    }
    val sql2 =
      """
        |insert overwrite table ads_qx_project_info_account_p partition (date_year,date_month)
        |select
        | id,
        | pid,
        | status,
        | cur_date,
        | cur_date_year,
        | cur_date_month,
        | amt,
        | create_user,
        | create_time,
        | last_modify_user,
        | last_modify_time,
        | is_deleted,
        | cur_date_year AS date_year,
        | cur_date_month AS date_month
        |from temp_project_info_account
        |
        |""".stripMargin

    try{
      import spark._
      val sqlstr = real_sql.substring(0,real_sql.length()-5)
      sql(sqlstr).createOrReplaceTempView("temp_project_info_account")
      sql(sql2)
    }finally {
      spark.stop()
    }

  }

}

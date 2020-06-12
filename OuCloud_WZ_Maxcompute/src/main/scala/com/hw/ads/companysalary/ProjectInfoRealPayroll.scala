package hw.ads.companysalary

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * --实名制-工资单年月
 *
 *  --数据来源:
 *      ods_base_project;
 *      ods_base_labourer_resume;
 *      ods_base_salary_batch;
 *      ods_base_salary_sub;
 * --获取字段
 * `pid -- 项目id',
 * `status--状态：1，未落实  2，已处理  3，已落实',
 * `cur_date--所属年月日期',
 * `cur_date_year--所属年',
 * `cur_date_month--所属月',
 * `real_count--实名制人数',
 * `pay_count----发放人数',
 * `amt----金额',
 * `create_user----创建人',
 * `create_time----创建时间',
 * `last_modify_user----修改人',
 * `last_modify_time-----修改时间',
 * `is_deleted-----是否删除 - 1:未删除 2:删除',
 */
object ProjectInfoRealPayroll extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    //获取需要统计的时间
    val dateMaps = spark.conf.get("spark.project.dateMaps")
    val dates = dateMaps.split(",")
    var real_sql = new StringBuffer()

    if (dates.size>1) {
      dateMaps.split(",").map(x => {
        val datetime = x
        // 读 普通表
        var sqlstr =
          s"""
             |select
             | concat(qp.pid,year('${datetime}'),month('${datetime}')) id,
             | qp.pid,
             | case when amt>0 then 3 else 1 end status,
             | trunc('${datetime}','MM')  cur_date,
             | year('${datetime}')  cur_date_year,
             | month('${datetime}') cur_date_month,
             | real_count,
             | pay_count,
             | amt,
             | '' create_user,
             | now() create_time,
             | '' last_modify_user,
             | now() last_modify_time,
             | qp.is_deleted
             |from
             | ods_base_project qp
             |left join (
             |   SELECT
             |	  a.source_pid,
             |	  COUNT(*) real_count
             | FROM (
             |	  SELECT
             |     DISTINCT id_card_no,
             |		  source_pid
             |	  FROM
             |		  ods_base_labourer_resume
             |	  WHERE is_deleted = 1
             |		  AND source_pid IN ( SELECT pid FROM ods_base_project )
             |		  AND endtime >= date_format('${datetime}','yyyy-MM-dd HH:mm:ss')
             |		  AND starttime < date_format ( add_months ( '${datetime}', 1 ), 'yyyy-MM-dd HH:mm:ss' )
             |   )a GROUP BY a.source_pid
             |
             |) RE on qp.pid = RE.source_pid
             |left join (
             |   SELECT
             |	    A.source_pid,
             |	    COUNT ( * ) pay_count
             | FROM (
             |	  SELECT
             |		  DISTINCT ss.id_card_no,
             |		  sb.source_pid
             |	  FROM
             |		  ods_base_salary_batch sb
             |		JOIN ods_base_salary_sub ss ON sb.batch_no = ss.batch_no
             |    and ss.is_deleted = 1
             |    AND ss.status = 1
             |	  WHERE sb.is_deleted = 1
             |		AND sb.status = 1
             |		AND sb.YEAR = year('${datetime}')
             |		AND sb.MONTH = month('${datetime}')
             |	 ) A  GROUP BY A.source_pid
             |) PA on qp.pid = PA.source_pid
             |left join (
             |   SELECT
             |	    sb.source_pid,
             |	    SUM ( real_amt ) amt
             |  FROM
             |   ods_base_salary_batch sb
             |  WHERE sb.is_deleted = 1
             |  AND sb.status = 1
             |	 AND sb.YEAR = year('${datetime}')
             |	 AND sb.MONTH = month('${datetime}')
             |  GROUP BY sb.source_pid
             |) SB on qp.pid = SB.source_pid
             |
             |""".stripMargin
        real_sql.append(sqlstr).append("union")
      })
    }else {
      val datetime = dateMaps
      var sqlstr =
        s"""
           |select
           | concat(pid,year('${datetime}'),month('${datetime}')) id,
           | pid,
           | case when amt>0 then 3 else 1 end status,
           | trunc('${datetime}','MM')  cur_date,
           | year('${datetime}')  cur_date_year,
           | month('${datetime}') cur_date_month,
           | real_count,
           | pay_count,
           | amt,
           | '' create_user,
           | now() create_time,
           | '' last_modify_user,
           | now() last_modify_time,
           | qp.is_deleted
           |from
           | ods_base_project qp
           |left join (
           |   SELECT
           |	  a.source_pid,
           |	  COUNT(*) real_count
           | FROM (
           |	  SELECT
           |     DISTINCT id_card_no,
           |		  source_pid
           |	  FROM
           |		  ods_base_labourer_resume
           |	  WHERE is_deleted = 1
           |		  AND source_pid IN ( SELECT pid FROM ods_base_project )
           |		  AND endtime >= date_format('${datetime}','yyyy-MM-dd HH:mm:ss')
           |		  AND starttime < date_format ( add_months ( '${datetime}', 1 ), 'yyyy-MM-dd HH:mm:ss' )
           |   )a GROUP BY a.source_pid
           |
           |) RE on qp.pid = RE.source_pid
           |left join (
           |   SELECT
           |	    A.source_pid,
           |	    COUNT ( * ) pay_count
           | FROM (
           |	  SELECT
           |		  DISTINCT ss.id_card_no,
           |		  sb.source_pid
           |	  FROM
           |		  ods_base_salary_batch sb
           |		JOIN ods_base_salary_sub ss ON sb.batch_no = ss.batch_no
           |	 WHERE sb.is_deleted = 1
           |		AND ss.is_deleted = 1
           |		AND ss.status = 1
           |		AND ss.status = 1
           |		AND sb.YEAR = year('${datetime}')
           |		AND sb.MONTH = month('${datetime}')
           |		AND sb.source_pid IN ( SELECT pid FROM ods_base_project )
           |	 ) A  GROUP BY A.source_pid
           |) PA on qp.pid = PA.source_pid
           |left join (
           |   SELECT
           |	    sb.source_pid,
           |	    SUM ( real_amt ) amt
           |  FROM
           |   ods_base_salary_batch sb
           |  WHERE sb.is_deleted = 1
           |  AND sb.status = 1
           |	 AND sb.YEAR = year('${datetime}')
           |	 AND sb.MONTH = month('${datetime}')
           |	 AND sb.source_pid IN ( SELECT pid FROM ods_base_project )
           |  GROUP BY sb.source_pid
           |) SB on qp.pid = SB.source_pid
           |
           |""".stripMargin
      real_sql.append(sqlstr).append("union")
    }
    val sql2 =
      """
        |insert overwrite table ads_qx_project_info_real_payroll_p partition (date_year,date_month)
        |select
        | id,
        | pid,
        | status,
        | cur_date,
        | cur_date_year,
        | cur_date_month,
        | ifnull(real_count,0) real_count,
        | ifnull(pay_count,0) pay_count,
        | ifnull(amt,0) amt,
        | create_user,
        | create_time,
        | last_modify_user,
        | last_modify_time,
        | is_deleted,
        | cur_date_year AS date_year,
        | cur_date_month AS date_month
        |from temp_real_payroll
        |
        |""".stripMargin

    try{
      import spark._
      val sqlstr = real_sql.substring(0,real_sql.length()-5)
      sql(sqlstr).createOrReplaceTempView("temp_real_payroll")
      sql(sql2)
    }finally {
      spark.stop()
    }

  }

}

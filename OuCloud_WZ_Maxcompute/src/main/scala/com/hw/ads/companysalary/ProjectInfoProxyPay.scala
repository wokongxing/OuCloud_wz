package hw.ads.companysalary

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * --银行代发工资-工资单年月
 *  --数据来源:
 *      ods_base_salary_batch
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
 * `is_deleted-------是否删除 - 1:未删除 2:删除',
 */
object ProjectInfoProxyPay extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sql2 =
      """
        |insert overwrite table ads_qx_project_info_proxy_pay
        |SELECT
        |   concat(source_pid,year,month) id,
        |   source_pid pid,
        |   case when SUM(amt)>0 then 3 else 1 end status,
        |	  trunc(year_month,'MM') cur_date,
        |	  year cur_date_year,
        |	  month cur_date_month,
        |	  SUM(amt) amt,
        |   '' as create_user,
        |   now() as create_time,
        |   '' as last_modify_user,
        |   now() as last_modify_time,
        |   1 as is_deleted
        |FROM
        |	  ods_base_salary_batch
        |WHERE is_deleted = 1
        |	 AND status = 1
        |GROUP BY
        |   trunc(year_month,'MM'),
        |   source_pid,
        |   year,
        |   month
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

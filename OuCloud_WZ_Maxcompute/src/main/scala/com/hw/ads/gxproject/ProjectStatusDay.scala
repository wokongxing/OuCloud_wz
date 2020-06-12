package hw.ads.gxproject

import hw.utils.SparkUtils

/**项目维度指标 项目状态-数据分析 -每天凌晨运行
 * 数据来源: CDM层
 *    cdm_gx_project_day-以天为维度 -项目信息表;
 * 获取字段:
 *    区域,年月日,
 *    项目总数-project_total;
 *    在建项目数-bulid_total;
 *    竣工项目数-complete_total;
 *    停工项目数-stop_total;
 *    涉及企业数量-companys_sum;
 *  存储位置:
 *    ads_gx_project_status_day
 * @author lzhy
 */
object ProjectStatusDay {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)



    val sql=
      """
        |insert overwrite table ads_gx_project_status_day
        |SELECT
        |	IFNULL( pd.county_name, '其他' ) county_name,
        |	pd.year,
        |	pd.month,
        |	pd.day,
        | pd.create_time,
        |	COUNT ( pd.pid ) project_total,
        |	SUM ( CASE WHEN project_condition = 1 THEN 1 ELSE 0 END ) AS bulid_total,
        |	SUM ( CASE WHEN project_condition = 2 THEN 1 ELSE 0 END ) AS stop_total,
        |	SUM ( CASE WHEN project_condition = 3 THEN 1 ELSE 0 END ) AS complete_total ,
        | COUNT( DISTINCT pd.cid ) AS companys_sum,
        | SUM ( pd.is_billboards ) billboards,
        |	SUM ( pd.project_account_status ) account_total
        |FROM
        |	cdm_gx_project_day pd
        |GROUP BY
        |	pd.county_name,
        |	pd.year,
        |	pd.month,
        |	pd.day,
        | pd.create_time
        |""".stripMargin

    val dataFrame = spark.sql(sql)

    spark.stop();
  }

}

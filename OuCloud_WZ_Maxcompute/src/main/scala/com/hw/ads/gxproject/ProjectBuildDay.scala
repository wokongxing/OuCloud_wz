package hw.ads.gxproject


import hw.utils.SparkUtils

/**项目维度指标 在建项目类型-数据分析 -每天凌晨运行
 * 数据来源: CDM层
 *    cdm_gx_project_day-以天为维度 -项目信息表;
 *    项目类型-(1-房屋;2-市政;3-附属;99-其他)
 * 获取字段:
 *    区域,年月日,
 *    在建项目数-bulid_total;
 *    市政项目数--municipal_total;
 *    房屋项目数--houses_total;
 *    附属项目数--sub_total;
 *    其他项目数--other_total;
 *    维权公示牌数--billboards;
 *    已实名项目数--realname_total;
 *    已备案专项账户项目数--account_total;
 *    已缴纳保障金项目数--safeguard_total;
 *    已实现云考勤项目数--attendance_total;
 *    已签订劳务合同项目数--contract_total;
 *    已签订专项监管三方协议项目数--agreement_total;
 *    以及各个所占比率;
 * 存储位置:
 *    ads_gx_project_build_day
 * @author lzhy
 */
object ProjectBuildDay {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)


    val sqlstr=
      """
        |insert overwrite table ads_gx_project_build_day
        |SELECT
        |	P.province_name,
        |	P.city_name,
        |	P.county_name,
        | p.year,
        | p.month,
        | p.day,
        | p.create_time,
        |	P.project_total,
        |	P.houses_total,
        |	concat ( round( P.houses_total / P.project_total * 100, 2 ), '%' ) houses_proportion,
        |	P.municipal_total,
        |	concat ( round( P.municipal_total / P.project_total * 100, 2 ), '%' ) municipal_proportion,
        |	P.sub_total,
        |	concat ( round( P.sub_total / P.project_total * 100, 2 ), '%' ) sub_proportion,
        |	P.other_total,
        |	concat ( round( P.other_total / P.project_total * 100, 2 ), '%' ) other_proportion,
        |	P.billboards,
        |	concat ( round( P.billboards / P.project_total * 100, 2 ), '%' ) billboards_proportion,
        |	P.realname_total,
        |	concat ( round( P.realname_total / P.project_total * 100, 2 ), '%' ) realname_proportion,
        |	P.account_total,
        |	concat ( round( P.account_total / P.project_total * 100, 2 ), '%' ) account_proportion,
        | P.safeguard_total,
        |	concat ( round( P.safeguard_total / P.project_total * 100, 2 ), '%' ) safeguard_proportion,
        |	P.attendance_total,
        |	concat ( round( P.attendance_total / P.project_total * 100, 2 ), '%' ) attendance_proportion,
        |	P.contract_total,
        |	concat ( round( P.contract_total / P.project_total * 100, 2 ), '%' ) contract_proportion,
        |	P.agreement_total,
        |	concat ( round( P.agreement_total / P.project_total * 100, 2 ), '%' ) agreement_proportion
        |FROM
        |	(
        |	SELECT
        |		IFNULL( pd.county_name, '其他' ) county_name,
        |   IFNULL( pd.city_name, '其他' ) city_name,
        |   IFNULL( pd.province_name, '其他' ) province_name,
        |   pd.year,
        |   pd.month,
        |   pd.day,
        |   pd.create_time,
        |		COUNT ( pd.pid ) project_total,
        |		SUM ( CASE WHEN pd.project_type = '房屋建设工程' THEN 1 ELSE 0 END ) houses_total,
        |		SUM ( CASE WHEN pd.project_type = '市政工程' THEN 1 ELSE 0 END ) municipal_total,
        |		SUM ( CASE WHEN pd.project_type = '附属工程' THEN 1 ELSE 0 END ) sub_total,
        |		SUM ( CASE WHEN pd.project_type = '其它' THEN 1 ELSE 0 END ) other_total,
        |		SUM ( pd.is_billboards ) billboards,
        |		SUM ( pd.is_realname ) realname_total,
        |		SUM ( pd.project_account_status ) account_total,
        |  	SUM ( pd.safeguard_status ) safeguard_total,
        |		SUM ( pd.is_attendance ) attendance_total,
        |		SUM ( pd.contract_status ) contract_total,
        |		SUM ( pd.is_agreement ) agreement_total
        |FROM
        |	cdm_gx_project_day pd
        |WHERE status in (3,4,5)
        |	and is_deleted=1
        | and project_condition=1
        | and project_type in ('房屋建设工程','附属工程','市政工程','其它')
        |GROUP BY
        |	pd.county_name,
        | pd.city_name,
        | pd.province_name,
        | pd.year,
        | pd.month,
        | pd.day,
        | pd.create_time
        |	) P
        |""".stripMargin

    try{
      import spark._
      sql(sqlstr)
    }finally {
      spark.stop()
    }
  }


}

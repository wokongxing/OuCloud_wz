package hw.ads.labours

import hw.utils.SparkUtils

/**
 * 统计
 *    工薪发薪模块 总统计
 *
 *数据来源: cdm 发薪明细聚合表
 *    cdm_gx_salary_detail
 * 获取字段:
 * `real_amt` ------------ '发薪总额',
 * `construction_amt`----------- '施工单位发薪总额',
 * `construction_amt_proportion` ---------- '施工单位发薪总额占比',
 * `labourer_amt` -------------- '劳务单位发薪总额',
 * `labourer_amt_proportion`-----'劳务单位发薪总额占比',
 * `labourer_count`--------------'发放人数',
 * `batch_sum` ------------------'发放批次数量',
 * `project_total`---------------'薪资发放项目数',
 * `build_project_total`----------'在建项目数',
 * `build_project_proportion`---- '在建项目数占比',
 * `complete_project_total`-------'竣工或停工项目数',
 * `complete_project_proportion`---'竣工或停工项目数占比',
 * `last_real_amt` ----------------'上个月发薪金额',
 * `last_construction_amt`---------'上个月施工单位发薪金额',
 * `last_construction_amt_proportion` ---- '上个月施工单位发薪金额占比',
 * `last_labourer_amt` -------------'上个月劳务公司发薪金额',
 * `last_labourer_amt_proportion`-----'上个月劳务公司发薪金额占比',
 * `last_labourer_count`------------'上个月发薪人数',
 * `last_batch_sum`-----------------'上个月发薪批次数量')
 *   落地 : http://668f26f2a89648cb996679d0eccaafdd-cn-hangzhou.alicloudapi.com/gx/labourer_total?appCode=f37c241337824b3fb0f0417bb9497e8c&project_type=%E5%B8%82%E6%94%BF%E5%B7%A5%E7%A8%8B&county_code=330302
 *    ads_gx_labourer_salary_total
 *
 * @author linzhy
 */
object LabourerSalaryTotal {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr =
      """
        |
        |insert overwrite table ads_gx_labourer_salary_total
        |select
        |	sa1.real_amt,
        |	sa1.Construction_Amt,
        |	CONCAT ( ROUND( sa1.Construction_Amt / sa1.real_amt * 100, 2 ), '%' ) Construction_Amt_proportion,
        |	sa1.labourer_Amt,
        |	CONCAT ( ROUND( sa1.labourer_Amt / sa1.real_amt * 100, 2 ), '%' ) labourer_Amt_proportion,
        |	sa1.labourer_count,
        |	sa2.batch_sum,
        |	sa3.project_total,
        |	sa3.build_project_total,
        |	CONCAT ( ROUND( sa3.build_project_total / sa3.project_total * 100, 2 ), '%' ) build_project_proportion,
        |	sa3.complete_project_total,
        |	CONCAT ( ROUND( sa3.complete_project_total / sa3.project_total * 100, 2 ), '%' ) complete_project_proportion,
        |	sa4.last_real_amt,
        |	sa4.last_Construction_Amt,
        |	CONCAT ( ROUND( sa4.last_Construction_Amt / sa4.last_real_amt * 100, 2 ), '%' ) last_Construction_Amt_proportion,
        |	sa4.last_labourer_Amt,
        |	CONCAT ( ROUND( sa4.last_labourer_Amt / sa4.last_real_amt * 100, 2 ), '%' ) last_labourer_Amt_proportion,
        |	sa4.last_labourer_count,
        |	sa5.last_batch_sum
        |from (
        |	select
        |		1 as linkid,
        |		sum(sa.real_amt) real_amt,
        |		sum(case when sa.sa_company_type=2 then sa.real_amt else 0 end) Construction_Amt,
        |		sum(case when sa.sa_company_type=3 then sa.real_amt else 0 end) labourer_Amt,
        |		count(DISTINCT sa.id_card_no) labourer_count
        |	from
        |		cdm_gx_salary_detail sa
        |	where sa.status=1
        |) sa1
        |	left join (
        |		select
        |			1 as linkid,
        |			count(DISTINCT sa.batch_no) batch_sum
        |		from
        |			cdm_gx_salary_detail sa
        |	) sa2 on sa1.linkid = sa2.linkid
        |	left join (
        |		select
        |			1 as linkid,
        |			count(sa.source_pid) project_total,
        |			sum(case when sa.project_condition=1 then 1 else 0 end) build_project_total,
        |			sum(case when sa.project_condition=3 or sa.project_condition=2 then 1 else 0 end) complete_project_total
        |		from (
        |			select
        |				source_pid,
        |				project_condition
        |			from
        |				cdm_gx_salary_detail
        |			group by source_pid,project_condition
        |		) sa
        |
        |	) sa3 on sa1.linkid = sa3.linkid
        |	left join (
        |		select
        |			1 as linkid,
        |			sum(sa.real_amt) last_real_amt,
        |			sum(case when sa.sa_company_type=2 then sa.real_amt else 0 end) last_Construction_Amt,
        |			sum(case when sa.sa_company_type=3 then sa.real_amt else 0 end) last_labourer_Amt,
        |			count(DISTINCT sa.id_card_no) last_labourer_count
        |		from
        |			cdm_gx_salary_detail sa
        |		where sa.status=1
        |		and sa.year = YEAR (add_months ( now(),- 1 ))
        |		and sa.month = month (add_months ( now(),- 1 ))
        |	) sa4 on sa1.linkid = sa4.linkid
        |	left join (
        |		select
        |			1 as linkid,
        |			count(DISTINCT sa.batch_no) last_batch_sum
        |		from
        |		cdm_gx_salary_detail sa
        |		where  sa.year = YEAR (add_months ( now(),- 1 ))
        |		and sa.month = month (add_months ( now(),- 1 ))
        |	) sa5 on sa1.linkid = sa5.linkid
        |
        |
        |""".stripMargin

    try {

      import spark._
      val dataFrame = sql(sqlstr)
      dataFrame.show(100)
      dataFrame.printSchema()
    }finally {
      spark.stop()
    }

  }


}

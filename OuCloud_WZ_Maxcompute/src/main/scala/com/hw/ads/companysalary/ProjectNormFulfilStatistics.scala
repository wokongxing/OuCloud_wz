package hw.ads.companysalary

import hw.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * --项目六项制度综合统计表
 *  --数据来源:
 *     ads_qx_project_norm_fulfil_new
 *
 *  --获取字段: project_norm_fulfil_new
 * `id` ,
 * `cur_date` date ----------- '所属年月日期',
 * `cur_date_year` int(11) ----- '所属年',
 * `cur_date_month` int(11) ----- '所属月',
 * `province_code` varchar(20) ----- '所属省',
 * `city_code` varchar(20)----------'所属市',
 * `county_code` varchar(20)------- '所属区',
 * `project_count` int(11) ---------'项目数',
 * `real_count` int(11)------------'实名制落实数量',
 * `real_proportion` decimal(16,2) -------'实名制落实占比',
 * `bail_count` int(11) ------------'工资支付保证金落实数量',
 * `bail_proportion` decimal(16,2) ------'工资支付保证金落实占比',
 * `accoun_count` int(11) ---------------'分账管理落实数量',
 * `accoun_proportion` decimal(16,2) ----- '分账管理落实占比',
 * `proxy_pay_count` int(11) ------------- '银行代发工资落实数量',
 * `proxy_pay_proportion` decimal(16,2) ---- '银行代发工资落实占比',
 * `billboard_count` int(11) -------------- '维权信息告示牌数量',
 * `billboard_proportion` decimal(16,2) ------ '维权信息告示牌占比',
 * `pay_count` int(11) ----------------------'按月足额支付工资数量',
 * `pay_proportion` decimal(16,2)------------ '按月足额支付工资占比',
 * `complete_count` int(11) -----------------'六项制度全落实项目数量',
 * `complete_proportion` decimal(16,2)----------'六项制度全落实项目占比',
 * `create_user` varchar(200)---------------- '创建人',
 * `create_time` datetime----------------------'创建时间',
 * `last_modify_user` varchar(200)------------- '修改人',
 * `last_modify_time` datetime---------------'修改时间',
 * `is_deleted` int(11) -------------------'1' COMMENT '是否删除 - 1:未删除 2:删除',
 * `sync_flag` varchar(200) --------------- '用作同步数据字段'
 *
 * @author linzhy
 */
object ProjectNormFulfilStatistics extends Logging{
  def main(args: Array[String]) {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |
        |insert overwrite table ads_qx_project_norm_fulfil_statistics
        |  select
        |  a.id,
        |  a.cur_date,
        |  a.cur_date_year,
        |  a.cur_date_month,
        |  a.province_code,
        |  a.city_code,
        |  a.county_code,
        |  a.industry_type,
        |  a.project_count,
        |  a.real_count,
        |  round(a.real_count / a.project_count * 100, 2 ) as real_proportion,
        |  a.bail_count,
        |  round(a.bail_count / a.project_count * 100, 2 ) as bail_proportion,
        |  a.accoun_count,
        |  round(a.accoun_count / a.project_count * 100, 2 ) as accoun_proportion,
        |  a.proxy_pay_count,
        |  round(a.proxy_pay_count / a.project_count * 100, 2 ) as proxy_pay_proportion,
        |  a.billboard_count,
        |  round(a.billboard_count / a.project_count * 100, 2 ) as billboard_proportion,
        |  a.pay_count,
        |  round(a.pay_count / a.project_count * 100, 2 ) as pay_proportion,
        |  a.complete_count,
        |  round(a.complete_count / a.project_count * 100, 2 ) as complete_proportion,
        |  a.create_user,
        |  a.create_time,
        |  a.last_modify_user,
        |  a.last_modify_time,
        |  a.is_deleted,
        |  a.sync_flag
        |  from (
        |    SELECT
        |    concat( city_code,county_code,year(now()),month(now()),industry_type ) as id
        |    ,trunc(now(),'MM')  cur_date
        |    ,year(now())  cur_date_year
        |    ,month(now()) cur_date_month
        |    ,province_code
        |    ,city_code
        |    ,county_code
        |    ,industry_type
        |    ,count(*) as project_count
        |    ,sum (case when real_status=3 then 1 else 0 end ) as real_count
        |    ,sum (case when bail_status=3 then 1 else 0 end ) as bail_count
        |    ,sum (case when account_status=3 then 1 else 0 end ) as accoun_count
        |    ,sum (case when proxy_pay_status=3 then 1 else 0 end ) as proxy_pay_count
        |    ,sum (case when billboard_status=3 then 1 else 0 end ) as billboard_count
        |    ,sum (case when pay_status=3 then 1 else 0 end ) as pay_count
        |    ,sum (case when real_status=3 and bail_status=3 and account_status=3 and proxy_pay_status=3
        |    and billboard_status=3 and pay_status=3 then 1 else 0 end ) as complete_count
        |    ,'' AS create_user
        |    ,now() AS create_time
        |    ,'' AS last_modify_user
        |    ,now() AS last_modify_time
        |    ,1 as is_deleted
        |    ,'无' as sync_flag
        |    FROM
        |    ads_qx_project_norm_fulfil_new
        |    where is_deleted=1
        |    and p_status=3
        |    group by province_code,city_code,county_code,industry_type
        |  ) a
        |
        |""".stripMargin

    try{
      import spark._
      sql(sqlstr)

    }finally {
      spark.stop()
    }

  }

}

package hw.cdm.gxcompany

import hw.utils.SparkUtils
/**
 * 数据来源:
 *    ods_gx_company ------企业;
 *    ods_gx_project ------源项目;
 *    ods_gx_project_sub --子项目;
 *    ods_gx_dict_area ----区域字典表;
 *  获取字段:
 *    cid --企业唯一标识;
 *    company_type ---------企业类型：1：建设集团，2：施工单位，3：劳务企业，4：设计单位;
 *    county_code ----------企业所在区域;
 *    project_type ---------项目类型 --房屋建筑工程,市政工程,附属工程,其他;
 *    project_city_code ----项目所在城市(即企业的业务所在城市);
 *    project_county_code --项目所在区域(即企业的业务所在区域);
 *    type_top -------------排行 (以cid,compnay_type 分组,项目类型倒序 排序,用于统计企业类型占比数量时, 去重)
 * 落地:
 *  cdm_gx_company_total_detail
 * @author linzhy
 */
object CompanyTotalDetail {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getSimpleName)

    val sqlstr=
      """
        |insert overwrite table cdm_gx_company_total_detail
        |select
        |   cid,
        |		company_type,
        |		county_code,
        |		project_type,
        |		project_city_code,
        |		project_county_code,
        |		type_top
        |from (
        | SELECT
        |		com.cid,
        |		com.company_type,
        |		com.county_code,
        |		pro.project_type,
        |		pro.city_code project_city_code,
        |		pro.county_code project_county_code,
        |		row_number() over(partition by com.cid,com.company_type order by pro.project_type ) type_top
        |	FROM (
        |		SELECT
        |			cid,
        |			county_code,
        |			company_type
        |		FROM
        |			ods_gx_company com
        |		WHERE is_deleted = 1
        |		AND STATUS = 3
        |	) com
        |	left JOIN (
        |		SELECT
        |			pro.cid,
        |			pro.project_type,
        |			pro.province_code,
        |			pro.city_code,
        |			pro.county_code
        |		FROM
        |			ods_gx_project pro
        |		WHERE pro.STATUS IN ( 3, 4, 5 )
        |		AND pro.is_deleted = 1
        |		UNION
        |		SELECT
        |			pro.unit_cid cid,
        |			pro.project_type,
        |			pro.province_code,
        |			pro.city_code,
        |			pro.county_code
        |		FROM
        |			ods_gx_project_sub pro
        |		WHERE pro.STATUS IN ( 3, 4)
        |		AND pro.is_deleted = 1
        |	) pro ON com.cid = pro.cid
        | ) a
        | where project_county_code in (select code from ods_gx_dict_area  where parentcode=330300 )
        |or county_code in (select code from ods_gx_dict_area  where parentcode=330300 )
        |""".stripMargin


    try{
      import spark._
      sql(sqlstr)
    }finally {
      spark.stop()
    }

  }

}

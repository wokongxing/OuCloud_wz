package main.scala.com.hw.utils

import java.io.{ByteArrayInputStream, Closeable, InputStream}
import java.sql.{Connection, PreparedStatement, SQLException}
import java.util.{Date, Locale, Properties}

import com.typesafe.config.{Config, ConfigFactory}
import main.scala.com.hw.utils.DateUtils.{SOURCE_TIME_FORMAT, logError}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.postgresql.copy.CopyManager
import org.postgresql.core.{BaseConnection}
import org.postgresql.ds.PGConnectionPoolDataSource


object PgSqlUtil extends Logging {
  /**
   * 保存数据到Pgsql--oucloud_ads(ADS层)
   * @param saveMode
   * @param dataFrame
   * @param config
   * @param dbtable
   */
  def saveDataToPgsqlAds(saveMode: SaveMode, dataFrame: DataFrame, config:Config, dbtable:String): Unit ={

    val ads_url = config.getString("pg.oucloud_ads.url")
    val ads_user = config.getString("pg.oucloud_ads.user")
    val ads_password = config.getString("pg.oucloud_ads.password")
    try{
      //保存数据到ads层
      dataFrame.write.format("jdbc")
        .mode(saveMode)
        .option("dbtable",dbtable)
        .option("url",ads_url)
        .option("user",ads_user)
        .option("password",ads_password)
        .save()
    }catch {
      case e: Exception =>
        logError(s"saveDataToPgsqlAds:TABLENAME $dbtable error: ${e.getMessage}")
        //保存
        insertError(connectionPool("OuCloud_ODS"),"saveDataToPgsqlAds",e.getMessage)
    }



  }

  /**
   * 保存数据到Pgsql--oucloud_cdm(CDM层)
   * @param saveMode
   * @param dataFrame
   * @param config
   * @param dbtable
   */
  def saveDataToPgsqlCdm(saveMode: SaveMode, dataFrame: DataFrame, config:Config, dbtable:String): Unit ={

    val cdm_url = config.getString("pg.oucloud_cdm.url")
    val cdm_user = config.getString("pg.oucloud_cdm.user")
    val cdm_password = config.getString("pg.oucloud_cdm.password")
    try{
      //保存数据到CDM层
      dataFrame.write.format("jdbc")
        .mode(saveMode)
        .option("url", cdm_url)
        .option("dbtable",dbtable)
        .option("user", cdm_user)
        .option("password", cdm_password)
        .save()
    }catch {
      case e: Exception =>
        logError(s"saveDataToPgsqlCdm:TABLENAME $dbtable error: ${e.getMessage}")
        //保存
        insertError(connectionPool("OuCloud_ODS"),"saveDataToPgsqlCdm",e.getMessage)
    }
  }

  /**
   * 通过copy 快速将文件导入数据库中
   * @param copyManager
   * @param sql
   * @param sb
   */
  def dataCopyToPgsql(copyManager:CopyManager, sql:String, sb:StringBuilder): Unit ={
    var instream: ByteArrayInputStream=null
    try{
      // 构建输入流
      instream = new ByteArrayInputStream(sb.toString().getBytes())
      copyManager.copyIn(sql,instream)

    }catch {
      case e: Exception =>
        logError(s"DataCopyToPgsql error: ${e.getMessage}")
        //保存
        insertError(connectionPool("OuCloud_ODS"),"DataCopyToPgsql",e.getMessage)
    }
    finally {
      IOUtils.closeStream(instream)
    }

  }

  /**
   * 异常信息保存
   * @param baseConnection
   * @param class_name
   * @param message
   */
  def insertError(baseConnection:BaseConnection,class_name:String,message:String) {

    val create_time=SOURCE_TIME_FORMAT.format(new Date(System.currentTimeMillis()))
    var sql =
      s"""
        |insert into test_error values('$create_time','$class_name','$message')
        |""".stripMargin
    if (baseConnection != null) {
      val statement = baseConnection.createStatement()
      try {
        statement.execute(sql)
      } catch {
        case e: Exception =>
          logError("Error in execution of insert. " + e.getMessage)
      } finally {
        statement.close()
        baseConnection.close()
      }
    }

    }

  /**
   * 批量插入 或更新 数据 ,该方法 借鉴Spark.write.save() 源码
   * @param conn
   * @param dataFrame
   * @param sc
   * @param table
   * @param id
   */
  def insertOrUpdateToPgsql(conn:BaseConnection,dataFrame:DataFrame,sc:SparkContext,table:String,id:String): Unit ={

    val tableSchema = dataFrame.schema
    val columns =tableSchema.fields.map(x => x.name).mkString(",")
    val placeholders = tableSchema.fields.map(_ => "?").mkString(",")
    val update = tableSchema.fields.map(x =>
      x.name.toString + "=?"
    ).mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders) on conflict($id) do update set $update"
    conn.setAutoCommit(false)
    val dialect = JdbcDialects.get(conn.getMetaData.getURL)
    val broad_cnn = sc.broadcast(conn)
    val batchSize = 2000
    try {
        dataFrame.foreachPartition(iterator =>{
          var rowCount = 0
          val ps = broad_cnn.value.prepareStatement(sql)
          val numFields = tableSchema.fields.length *2
          val updateindex = numFields / 2
          val nullTypes = tableSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
          val setters = tableSchema.fields.map(f => makeSetter(broad_cnn.value,f.dataType))
          try {
            //遍历批量提交
            iterator.foreach(row => {
              var i = 0
              while (i < numFields) {
                i < updateindex match {
                  case true => {
                    if (row.isNullAt(i)) {
                      ps.setNull(i + 1, nullTypes(i))
                    } else {
                      setters(i).apply(ps, row, i, 0)
                    }
                  }
                  case false => {
                    if (row.isNullAt(i - updateindex)) {
                      ps.setNull(i + 1, nullTypes(i - updateindex))
                    } else {
                      setters(i - updateindex).apply(ps, row, i, updateindex)
                    }
                  }
                }
                i = i + 1
              }
              ps.addBatch()
              rowCount += 1
              if (rowCount % batchSize == 0) {
                ps.executeBatch()
                rowCount = 0
              }
            })
            if (rowCount > 0) {
              ps.executeBatch()
            }

          }finally {
            ps.close()
          }
        })
      conn.commit()
    }catch {
      case e: Exception =>
        logError("Error in execution of insert. " + e.getMessage )
        var a = e
        conn.rollback()
        insertError(connectionPool("OuCloud_ODS"),"insertOrUpdateToPgsql",e.getMessage)
    }finally {
      if (conn!=null){
        conn.close()
      }

    }
  }
  /**
   *
   * @param baseConnection
   */
  def inser(baseConnection:BaseConnection) {

    var sql =
      s"""
         |insert into test_001_copy1(id,name,age) values(?,?,?)
         |on conflict(id) do update set id=?,name=?,age=?
         |""".stripMargin
    if (baseConnection != null) {
      val ps = baseConnection.prepareStatement(sql)
      try {
        ps.setInt(1,1)
        ps.setString(2,"22")
        ps.setInt(3,22)
        ps.setInt(4,1)
        ps.setString(5,"22")
        ps.setInt(6,22)
        ps.execute()

      } catch {
        case e: Exception =>
          logError("Error in execution of insert. " + e.getMessage)
      } finally {
        ps.close()
        baseConnection.close()
      }
    }

  }
  /**
   * 创建PGsql数据库连接池
   * @param databaseName
   * @return
   */
  def connectionPool(databaseName:String="OuCloud_ODS",
                     ip:Array[String]=Array("192.168.1.45"),
                     user:String="postgres",
                     password:String="22816010",
                     portnumbers:Array[Int]=Array(5432)): BaseConnection ={

    val source = new PGConnectionPoolDataSource
    source.setServerNames(ip)
    source.setPortNumbers(portnumbers)
    source.setDatabaseName(databaseName)
    source.setUser(user)
    source.setPassword(password)

    val baseConnection = source.getConnection.getMetaData.getConnection.asInstanceOf[BaseConnection]
    baseConnection
  }


  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.catalogString}"))
  }

  private type JDBCValueSetter_add = (PreparedStatement, Row, Int,Int) => Unit
  private def makeSetter(
                          conn: Connection,
                          dataType: DataType): JDBCValueSetter_add = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setInt(pos + 1, row.getInt(pos-currentpos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setLong(pos + 1, row.getLong(pos-currentpos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos-currentpos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos-currentpos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setInt(pos + 1, row.getShort(pos-currentpos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setInt(pos + 1, row.getByte(pos-currentpos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos-currentpos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setString(pos + 1, row.getString(pos-currentpos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos-currentpos))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos-currentpos))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos-currentpos))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos-currentpos))
    case _ =>
      (stmt: PreparedStatement, row: Row, pos: Int,currentpos:Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  /**
   * 测试用例
   * @param args
   */
  def main(args: Array[String]): Unit = {

    //连接数据库
    val baseConnection = connectionPool("OuCloud_ODS")

//    //创建 copyManager
//    val copyManager = new CopyManager(baseConnection)
//    val sb: StringBuilder = new StringBuilder()
//    sb.append(3).append(",")
//      .append("2").append(",")
//      .append(3).append("\n")
//
//    dataCopyToPgsql(copyManager,"COPY test_001 FROM STDIN WITH CSV",sb)

    //测试 insertOrUpdate
    inser(baseConnection)
//    insertError(connectionPool("OuCloud_ODS"),"DataCopyToPgsql","e.getMessage")

  }

}

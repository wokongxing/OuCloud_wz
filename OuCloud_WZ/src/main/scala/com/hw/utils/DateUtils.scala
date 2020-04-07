package main.scala.com.hw.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.internal.Logging

object DateUtils extends Logging {
  //不使用 java 自带的SimpleDateFormat 类,具有线程安全问题

  val SOURCE_TIME_FORMAT =FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_TIME_FORMAT =  FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time: String) = {
    try {
      SOURCE_TIME_FORMAT.parse(time).getTime
    } catch {
      case e: Exception =>
        logError(s"$time parse error: ${e.getMessage}")
        0l
    }
  }

  def parseToMinute(time: String) = {
    TARGET_TIME_FORMAT.format(new Date(getTime(time)))
  }

  def getDay(minute: String) = {
    minute.substring(0, 8)
  }

  def getHour(minute: String) = {
    minute.substring(8, 10)
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2019-11-30 10:14:09"))
    println(getDay(parseToMinute("2019-11-30 10:14:09")))
    println(getHour(parseToMinute("2019-11-30 10:14:09")))
    println(getTime("2019-11-30 10:14:09"))
    println(SOURCE_TIME_FORMAT.format(new Date(System.currentTimeMillis())))
  }
}

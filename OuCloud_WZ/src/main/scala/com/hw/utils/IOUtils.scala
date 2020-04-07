package main.scala.com.hw.utils

import java.io.Closeable

import org.apache.spark.internal.Logging

object IOUtils extends Logging{
  /**
   * IO关闭
   * @param closeable
   */
  def closeStream(closeable:Closeable): Unit ={
    if (closeable!=null) {
      try {
        closeable.close()
      } catch {
        case e: Exception =>
          logError(s"closeable error: ${e.getMessage}")
      }
    }

  }

}

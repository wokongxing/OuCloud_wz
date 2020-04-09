package hw.monitor

import org.apache.spark.{ExceptionFailure, SparkConf, TaskFailedReason, TaskKilled}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

/**
 * demo:
 *    继承SparkListener监听类 实现spark作业信息的获取,对作业预警 页面展示等
 * @author linzy
 * @param sparkconf
 */
class TestMonitoring(sparkconf: SparkConf) extends SparkListener with Logging{
  //任务结束的事件
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    if (taskEnd.taskInfo != null && taskEnd.stageAttemptId != -1){
      //作业Fail,kill,发生异常,将发送预警信息
        val errorMessage :Option[String]=
          taskEnd.reason match {
            case kill : TaskKilled=>
                Some(kill.toErrorString)
            case e : ExceptionFailure =>
                Some(e.toErrorString)
            case e: TaskFailedReason =>
                Some(e.toErrorString)
            case _ => None
          }

      //依据配置参数,可以动态的调整是否要开启作业的监控
      if (sparkconf.getBoolean("spark.sendEmail.OnTaskFail.enabled",false)){
        if (errorMessage.nonEmpty){
          println("------------------------发送预警邮件等等操作-------------------------")
        }

        println("------------------------监控到数据啦----保存数据到数据库等操作-----------------------------")

        val str =
          s"""
             |task任务名称:--------------${sparkconf.get("spark.app.name")}
             |task任务stageid:-----------${taskEnd.stageId}
             |task任务stageAttemptId:----${taskEnd.stageAttemptId}
             |task任务状态:---------------${taskEnd.reason}
             |task任务taskType:-----------${taskEnd.taskType}
             |----------------------------------taskInfo信息---------------------------------
             |
             |task任务excutorid:-------${taskEnd.taskInfo.executorId}
             |task任务是否失败:---------${taskEnd.taskInfo.failed}
             |task任务运行的所在host:---------${taskEnd.taskInfo.host}
             |task任务运行finishTime:---------${taskEnd.taskInfo.finishTime}
             |----------------------------------taskMetrics-task指标-------------------------------------
             |
             |task任务数据resultSize:---------${taskEnd.taskMetrics.resultSize}
             |task任务excutor结果集序列化所用时间:---------${taskEnd.taskMetrics.resultSerializationTime}
             |task任务数据executorCpuTime:---------${taskEnd.taskMetrics.executorCpuTime}
             |task任务excutor运行时间:---------${taskEnd.taskMetrics.executorRunTime}
             |task任务excutor反序列化所用cpu时间:---------${taskEnd.taskMetrics.executorDeserializeCpuTime}
             |task任务excutor反序列化所用时间:---------${taskEnd.taskMetrics.executorDeserializeTime}
             |task任务excutor数据溢写到磁盘的大小:---------${taskEnd.taskMetrics.diskBytesSpilled}
             |-----The number of in-memory bytes spilled by this task.
             |task任务excutor内存溢写的大小:---------${taskEnd.taskMetrics.memoryBytesSpilled}
             |task任务excutor内存大小:---------${taskEnd.taskMetrics.peakExecutionMemory}
             |--可以通过配置,关闭,减少内存的使用(跟踪block状态,会占用过多的内存使用)
             |task任务修改的block状态信息集合:---------${taskEnd.taskMetrics.updatedBlockStatuses}
             |task任务JVM GC耗时:---------${taskEnd.taskMetrics.jvmGCTime}
             |
             |----------------------------------taskMetrics-task指标outputMetrics/inputMetrics-------------------------------------
             |task任务读取的数据量条数:---------${taskEnd.taskMetrics.inputMetrics.recordsRead}
             |task任务读取数据量大小:---------${taskEnd.taskMetrics.inputMetrics.bytesRead}
             |task任务输出数据条数:---------${taskEnd.taskMetrics.outputMetrics.recordsWritten}
             |task任务输出数据量大小:---------${taskEnd.taskMetrics.outputMetrics.bytesWritten}
             |
             |-------------------------------------taskMetrics-task-shuffle指标--------------------------------------
             |-----------------------------shuffleReadMetrics---------------------
             |task任务shuffle读取数据的条数:---------${taskEnd.taskMetrics.shuffleReadMetrics.recordsRead}
             |task任务拉取数据的等待时间:---------${taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime}
             |task任务本地块拉取的数量:---------${taskEnd.taskMetrics.shuffleReadMetrics.localBlocksFetched}
             |task任务读取本地数据的大小字节:---------${taskEnd.taskMetrics.shuffleReadMetrics.localBytesRead}
             |task任务远程拉取数据块数量:---------${taskEnd.taskMetrics.shuffleReadMetrics.remoteBlocksFetched}
             |task任务远程读取数据的字节数:---------${taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesRead}
             |task任务远程读取数据到磁盘的字节数:---------${taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk}
             |task任务shuffle拉取块总数:---------${taskEnd.taskMetrics.shuffleReadMetrics.totalBlocksFetched}
             |task任务shuffle读取数据总字节大小:---------${taskEnd.taskMetrics.shuffleReadMetrics.totalBytesRead}
             |--------------------------------shuffleWriteMetrics-------------------
             |task任务shuffle写到磁盘或内存所用时间:---------${taskEnd.taskMetrics.shuffleWriteMetrics.writeTime}
             |task任务shuffle写数据字节大小:---------${taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten}
             |task任务shuffle写数据条数:---------${taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten}
             |
             |""".stripMargin

        println(str)

      }


    }

  }
}

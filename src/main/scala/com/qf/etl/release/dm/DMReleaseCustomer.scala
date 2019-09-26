package com.qf.etl.release.dm

import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.constant.ReleaseConstant
import com.qf.etl.release.dw.DWReleaseCustomer
import com.qf.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * DM投放目标目标客户主题
  */
object DMReleaseCustomer {
  //日志处理
  private val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

  def handleReleaseJob(spark: SparkSession, appName: String, bdp_date: String) = {
    //获取当前时间
    val begin = System.currentTimeMillis()

    try{
      //导入隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //设置缓存级别
      val storagelevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite
      //获取日志字段数据
      val customerColumns = DMReleaseColumnsHelper.selectDMReleaseCustomerColumns()
      val column = col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_date)

      val df: DataFrame = SparkHelper.readTableData(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,customerColumns)
          .where(column)
      df.show(10,false)
//      val customerReleaseDF = df.groupBy(
//        col(s"${ReleaseConstant.COL_RLEASE_SOURCES}"),
//        col(s"${ReleaseConstant.COL_RLEASE_CHANNELS}"),
//        col(s"${ReleaseConstant.COL_RLEASE_DEVICE_TYPE}"),
//        col(s"${ReleaseConstant.DEF_PARTITION}")).agg(expr("count(distinct idcard)") as "user_count",expr("count(release_session)") as "total_count")
      println("DWReleaseDF=====================================")
      // 打印查看结果
      //customerReleaseDF.show(10,false)
      // 目标用户（存储）
      //SparkHelper.writeTableData(customerReleaseDF,ReleaseConstant.DM_CUSTOMER_SOURCES,saveMode)
    }catch {
      // 错误信息处理
      case ex:Exception => {
        logger.error(ex.getMessage, ex)
      }
    }
  }




  def handleJobs(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
    var spark: SparkSession = null
    try {
      //配置Spark参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName(appName)
        .setMaster("local[*]")
      //创建上下文
      spark = SparkHelper.createSpark(conf)
      //解析参数
      val timeRange = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      //循环参数
      for (bdp_day <- timeRange) {
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark, appName, bdp_date)
      }
   }
  }

  def main(args: Array[String]): Unit = {
    val appName ="dM_release_job"
    val bdp_day_begin="2019-09-24"
    val bdp_day_end = "2019-09-24"
    //执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }
}

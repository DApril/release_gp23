package com.qf.util

import com.qf.bigdata.release.util.DateUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * Spark工具类
  */
object SparkHelper {
  def writeTableData(sourceDF: DataFrame, table: String, mode: SaveMode) = {
    sourceDF.write.mode(mode).insertInto(table)
  }

  def readTableData(spark: SparkSession, tableName: String, colNames: ArrayBuffer[String]):DataFrame = {
    import spark.implicits._
    // 获取数据
    val tableDF = spark.read.table(tableName)
      .selectExpr(colNames:_*)
    tableDF
  }

  private val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

  /**
    * 创建SparkSession
    */
  def createSpark(conf:SparkConf):SparkSession={
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    spark
  }

  /**
    *参数校验
    */
  def rangeDates(begin:String,end:String):Seq[String]={
    val bdp_days = new ArrayBuffer[String]()
    try{
       val bdp_date_begin = DateUtil.dateFromat4String(begin,"yyyy-MM-dd")
       val bdp_date_end = DateUtil.dateFromat4String(end,"yyyy-MM-dd")
       //如果两个时间相等，取其中的第一个开始时间
       //如果不相等，计算时间差
      if(begin.equals(end)){
        bdp_days+=bdp_date_begin
      }else{
        var cday = bdp_date_begin
        while(cday!=bdp_date_end){
          bdp_days+=cday
          // 让初始时间累加，以天为单位
          cday = DateUtil.dateFromat4StringDiff(cday,1)
        }
      }
    }catch {
      case ex:Exception=>{
        println("参数不匹配")
        logger.error(ex.getMessage,ex)
      }
    }
    bdp_days
  }
}

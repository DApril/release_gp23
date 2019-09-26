package com.qf.constant

import org.apache.spark.storage.StorageLevel

object ReleaseConstant {
  // partition
  val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK
  val DEF_PARTITION:String = "bdp_day"
  val DEF_SOURCE_PARTITION = 4


  // 维度列
  val COL_RLEASE_SESSION_STATUS:String = "release_status"
  val COL_RLEASE_SOURCES:String="sources"
  val COL_RLEASE_CHANNELS:String="channels"
  val COL_RLEASE_DEVICE_TYPE:String="device_type"
  

  // ods================================
  val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

  // dw=================================
  val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"

  //dm==================================
  val DM_CUSTOMER_SOURCES = "dm_release.dm_customer_sources"

}

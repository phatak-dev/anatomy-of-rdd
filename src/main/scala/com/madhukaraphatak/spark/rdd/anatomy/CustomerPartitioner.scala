package com.madhukaraphatak.spark.rdd.anatomy

import org.apache.spark.Partitioner

/**
 * Created by madhu on 11/3/15.
 */
class CustomerPartitioner extends Partitioner {
  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int =
    key match {
      case null => 0
      case _ => {
        val keyValue = key.toString.toInt
        if(keyValue>=2) 1 else 0
      }
    }
}

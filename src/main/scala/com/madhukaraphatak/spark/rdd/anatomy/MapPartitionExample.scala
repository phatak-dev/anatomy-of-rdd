package com.madhukaraphatak.spark.rdd.anatomy

import org.apache.spark.SparkContext

/**
 * Created by madhu on 11/3/15.
 */
object MapPartitionExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"map partition example")
    val salesData = sc.textFile(args(1))

    val (min,max)=salesData.mapPartitions(iterator => {
      val (min,max) = iterator.foldLeft((Double.MaxValue,Double.MinValue))((acc,salesRecord) => {
        val itemValue = salesRecord.split(",")(3).toDouble
        (acc._1 min itemValue , acc._2 max itemValue)
      })
      List((min,max)).iterator
    }).reduce((a,b)=> (a._1 min b._1 , a._2 max b._2))
    println("min = "+min + " max ="+max)

  }

}

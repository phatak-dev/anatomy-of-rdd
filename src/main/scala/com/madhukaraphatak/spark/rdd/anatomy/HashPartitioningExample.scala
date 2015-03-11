package com.madhukaraphatak.spark.rdd.anatomy

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by madhu on 11/3/15.
 */
object HashPartitioningExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"hash partitioning example")

    val salesData = sc.textFile(args(1))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1),colValues(3).toDouble)
    })

    val groupedData = salesByCustomer.groupByKey()

    println("default partitions are "+groupedData.partitions.length)


    //increase partitions
    val groupedDataWithMultiplePartition = salesByCustomer.groupByKey(2)
    println("increased partitions are " + groupedDataWithMultiplePartition.partitions.length)

  }

}

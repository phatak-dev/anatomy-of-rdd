package com.madhukaraphatak.spark.rdd.anatomy

import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.SparkContext._



/**
 * Created by madhu on 11/3/15.
 */
object LocationAwareness {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"location awareness example")

    val salesData = sc.textFile(args(1))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1),colValues(3).toDouble)
    })

    val groupedData = salesByCustomer.groupByKey(new CustomerPartitioner)

    groupedData.cache()

    for (partition <- groupedData.partitions) {

     val method = Class.forName("org.apache.spark.rdd.RDD").getMethod("getPreferredLocations", classOf[Partition])

     method.setAccessible(true)

     println(method.invoke(groupedData,partition))

    }





  }

}

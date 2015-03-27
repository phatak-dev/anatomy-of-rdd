package com.madhukaraphatak.spark.rdd.anatomy.extend


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object DiscountRDDExample {

  class CustomFunctions(rdd:RDD[SalesRecord]) {

    def discount(discountPercentage:Double) = new DiscountRDD(rdd,discountPercentage)

  }

  object CustomFunctions {

    implicit def addCustomFunctions(rdd: RDD[SalesRecord]) = new CustomFunctions(rdd)
  }

  def main (args: Array[String]) {

    val sc = new SparkContext(args(0), "discountRDD")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0),colValues(1),colValues(2),colValues(3).toDouble)
    })



    import CustomFunctions._
    val discountRDD = salesRecordRDD.discount(0.1)
    println(discountRDD.collect().toList)



  }





}

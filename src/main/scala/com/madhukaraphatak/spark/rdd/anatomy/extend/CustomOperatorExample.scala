package com.madhukaraphatak.spark.rdd.anatomy.extend



import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object CustomOperatorExample {

  class CustomFunctions(rdd:RDD[SalesRecord]) {

    def totalSales = rdd.map(_.itemValue).sum


  }

  object CustomFunctions {

    implicit def addCustomFunctions(rdd: RDD[SalesRecord]) = new CustomFunctions(rdd)
  }

  def main (args: Array[String]) {

    val sc = new SparkContext(args(0), "extendingspark")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0),colValues(1),colValues(2),colValues(3).toDouble)
    })

    println("built in operator sum is "+salesRecordRDD.map(_.itemValue).sum)

    import CustomFunctions._
    println("custom operator sum is "+salesRecordRDD.totalSales)



  }





}

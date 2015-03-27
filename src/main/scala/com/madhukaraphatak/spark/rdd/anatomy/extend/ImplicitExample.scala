package com.madhukaraphatak.spark.rdd.anatomy.extend

/**
 * Created by madhu on 27/3/15.
 */
object ImplicitExample {

  class CustomIntFunctions (value:Int) {

    def square = value*value
  }

  object CustomIntFunctions {
    implicit def addCustomFunctions(value:Int) = new CustomIntFunctions(value)
  }


  def main(args: Array[String]) {

    val normalInt = 10
    import CustomIntFunctions._

    println(" the square of the value" +  normalInt.square)

    /*
        The following gives error as the square method is only available on Int's
        val float = 10.0
        println(" the square of the value" +  float.square)
     */
  }



}

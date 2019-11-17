package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: takeOrdered算子
  * version: 1.0
  */
object Spark35_TakeOrdered {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：返回一个由RDD的前n个元素组成的数组

        2. 需求：创建一个RDD，统计该RDD的条数
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("takeOrdered")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(Array(2, 5, 4, 6, 8, 3))

        // (num: Int)(implicit ord: Ordering[T])
        val result: Array[Int] = rdd1.takeOrdered(3)
        println(result.mkString(", ")) //2, 3, 4

        sc.stop()
    }
}

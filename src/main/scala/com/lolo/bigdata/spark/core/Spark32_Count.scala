package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: count算子
  * version: 1.0
  */
object Spark32_Count {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：返回RDD中元素的个数

        2. 需求：创建一个RDD，统计该RDD的条数
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("count")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2)

        val result: Long = rdd1.count()
        println(result) //10

        sc.stop()
    }
}

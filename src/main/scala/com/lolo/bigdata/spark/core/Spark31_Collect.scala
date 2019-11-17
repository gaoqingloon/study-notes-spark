package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: collect算子
  * version: 1.0
  */
object Spark31_Collect {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：在驱动程序中，以数组的形式返回数据集的所有元素。

        2. 需求：创建一个RDD，并将RDD内容收集到Driver端打印
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("collect")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2)

        val result: Array[Int] = rdd1.collect()
        println(result.mkString(", ")) //1, 2, 3, 4, 5, 6, 7, 8, 9, 10

        sc.stop()
    }
}

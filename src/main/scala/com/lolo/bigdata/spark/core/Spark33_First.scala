package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: first算子
  * version: 1.0
  */
object Spark33_First {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：在驱动程序中，以数组的形式返回数据集的所有元素。

        2. 需求：创建一个RDD，并将RDD内容收集到Driver端打印
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("first")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2)

        val result: Int = rdd1.first()
        println(result) //1

        sc.stop()
    }
}

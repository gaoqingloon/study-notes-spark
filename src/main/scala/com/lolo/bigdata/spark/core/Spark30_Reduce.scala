package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: reduce算子
  * version: 1.0
  */
object Spark30_Reduce {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据。

        2. 需求：创建一个RDD，将所有元素聚合得到结果。
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("reduce")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2)
        val rdd2: RDD[(String, Int)] =
            sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))

        // f: (T, T) => T
        //val result: Int = rdd1.reduce((x, y) => x + y)
        val result: Int = rdd1.reduce(_ + _)
        println(result) //55

        val result2: (String, Int) = rdd2.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        println(result2) //(aadc,12)

        sc.stop()
    }
}

package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: countByKey算子
  * version: 1.0
  */
object Spark41_CountByKey {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：针对(K,V)类型的RDD，返回一个(K,Int)的map，
            表示每一个key对应的元素个数。

        2. 需求：创建一个PairRDD，统计每种key的个数
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("countByKey")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[(Int, Int)] =
            sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

        // 统计每种key的个数
        val count: collection.Map[Int, Long] = rdd1.countByKey()
        println(count.mkString(", ")) //3 -> 2, 1 -> 3, 2 -> 1

        sc.stop()
    }
}

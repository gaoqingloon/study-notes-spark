package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: flatMap算子
  * version: 1.0
  */
object Spark05_FlatMap {

    def main(args: Array[String]): Unit = {

        /*
        类似于map，但是每一个输入元素可以被映射为0或多个输出元素,
        所以func应该返回一个序列，而不是单一元素
         */
        val conf = new SparkConf()
            .setAppName("flatMap").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

        // f: T => TraversableOnce[U]
        val flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)

        flatMapRDD.collect().foreach(println)
        sc.stop()
    }
}

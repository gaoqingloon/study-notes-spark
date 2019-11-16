package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: intersection算子
  * version: 1.0
  */
object Spark17_Intersection {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：对源RDD和参数RDD求交集后返回一个新的RDD

        2. 需求：创建两个RDD，求两个RDD的交集
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("intersection")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD1: RDD[Int] = sc.makeRDD(1 to 7)
        val listRDD2: RDD[Int] = sc.makeRDD(5 to 10)

        // other: RDD[T]
        val intersectionRDD: RDD[Int] = listRDD1.intersection(listRDD2)
        val resArray: Array[Int] = intersectionRDD.collect()
        resArray.foreach(x => print(x + ", "))
        //intersectionRDD.collect().foreach(x => print(x + ", "))
        println()
        //5, 6, 7,

        sc.stop()
    }
}

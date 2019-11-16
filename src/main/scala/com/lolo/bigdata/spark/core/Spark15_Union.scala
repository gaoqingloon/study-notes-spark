package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: union算子
  * version: 1.0
  */
object Spark15_Union {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：对源RDD和参数RDD求并集后返回一个新的RDD

        2. 需求：创建两个RDD，求并集
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("union")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
        val listRDD2: RDD[Int] = sc.makeRDD(5 to 10)

        // other: RDD[T]
        val unionRDD: RDD[Int] = listRDD1.union(listRDD2)
        val resArray: Array[Int] = unionRDD.collect()
        resArray.foreach(x => print(x + ", "))
        //unionRDD.collect().foreach(x => print(x + ", "))
        println()
        //1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10,

        sc.stop()
    }
}

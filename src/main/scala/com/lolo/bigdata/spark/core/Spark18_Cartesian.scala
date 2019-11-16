package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: cartesian算子
  * version: 1.0
  */
object Spark18_Cartesian {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：笛卡尔积（尽量避免使用）

        2. 需求：创建两个RDD，计算两个RDD的笛卡尔积
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("cartesian")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD1: RDD[Int] = sc.makeRDD(1 to 3)
        val listRDD2: RDD[Int] = sc.makeRDD(2 to 5)

        // other: RDD[U]
        val cartesianRDD: RDD[(Int, Int)] = listRDD1.cartesian(listRDD2)
        val resTupleArray: Array[(Int, Int)] = cartesianRDD.collect()

        resTupleArray.foreach(x => print(x + ", "))
        //resTupleArray.collect().foreach(x => print(x + ", "))
        println()
        //(1,2), (1,3), (1,4), (1,5), (2,2), (2,3), (2,4), (2,5), (3,2), (3,3), (3,4), (3,5),

        sc.stop()
    }
}

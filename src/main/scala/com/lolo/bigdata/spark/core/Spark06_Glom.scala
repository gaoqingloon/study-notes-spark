package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: glom算子
  * version: 1.0
  */
object Spark06_Glom {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
            以分区为单位的统计
        2. 需求：创建一个4个分区的RDD，并将每个分区的数据放到一个数组
         */
        val conf = new SparkConf()
            .setAppName("glom").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(0 to 9, 3)

        val glomRDD: RDD[Array[Int]] = listRDD.glom()

        glomRDD.collect().foreach(array => println(array.mkString(",")))

        /*
        0,1,2
        3,4,5
        6,7,8,9
         */
        sc.stop()
    }
}

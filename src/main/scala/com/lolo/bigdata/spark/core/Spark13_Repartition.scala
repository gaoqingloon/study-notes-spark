package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: repartition算子
  * version: 1.0
  */
object Spark13_Repartition {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：根据分区数，重新通过网络随机洗牌所有数据。

        2. 需求：创建一个4个分区的RDD，对其重新分区
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("repartition")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
        println(listRDD.partitions.length)//4
        //以数组形式收集每个分区数据
        val preRepartition: Array[Array[Int]] = listRDD.glom().collect()
        preRepartition.foreach(x => println(x.mkString(", ")))
        /*
        1, 2, 3, 4
        5, 6, 7, 8
        9, 10, 11, 12
        13, 14, 15, 16
         */

        // repartition算子存在shuffle
        // numPartitions: Int
        // repartition实际上是调用的coalesce，默认是进行shuffle的
        // 底层调用：coalesce(numPartitions, shuffle = true)
        val repartitionRDD: RDD[Int] = listRDD.repartition(2)
        println(repartitionRDD.partitions.length)//2
        repartitionRDD.glom().collect().foreach(x => println(x.mkString(", ")))
        /*
        1, 3, 5, 7, 9, 11, 13, 15
        2, 4, 6, 8, 10, 12, 14, 16
         */
        sc.stop()
    }
}

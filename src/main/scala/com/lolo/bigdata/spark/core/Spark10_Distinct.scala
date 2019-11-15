package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: distinct 算子（产生shuffle）
  * version: 1.0
  */
object Spark10_Distinct {

    def main(args: Array[String]): Unit = {

        /*
        distinct([numTasks])) 案例
        1. 作用：对源RDD进行去重后返回一个新的RDD。默认情况下，
        只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它。

        2. 需求：创建一个RDD，使用distinct()对其去重。
         */
        val conf = new SparkConf()
            .setAppName("distinct").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1))

        // 默认为产生内核数个分区 distinct(partitions.length)
        //val distinctRDD: RDD[Int] = listRDD.distinct() //4个分区

        // 使用distinct算子对数据去重，但是因为去重后会导致数据减少，
        // 所以可以改变默认的分区数量
        // numPartitions: Int
        val distinctRDD: RDD[Int] = listRDD.distinct(2) //2个分区

        //distinctRDD.collect().foreach(println)
        distinctRDD.saveAsTextFile("output")

        sc.stop()
    }
}

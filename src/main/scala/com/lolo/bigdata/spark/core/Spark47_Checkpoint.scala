package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: checkpoint RDD缓存（磁盘）
  * version: 1.0
  */
object Spark47_Checkpoint {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("checkpoint")
        val sc = new SparkContext(conf)

        // 设置检查点目录
        sc.setCheckpointDir("checkpoint")

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

        // 对这个rdd做检查点，推荐checkpoint和cache一起使用
        //mapRDD.cache()
        mapRDD.checkpoint()

        val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

        reduceRDD.foreach(println)
        println(reduceRDD.toDebugString)
        /*
        //第一次没有checkpoint
        (4) ShuffledRDD[2] at reduceByKey at Spark50_Checkpoint.scala:21 []
         +-(4) MapPartitionsRDD[1] at map at Spark50_Checkpoint.scala:20 []
            |  ParallelCollectionRDD[0] at makeRDD at Spark50_Checkpoint.scala:19 []

        //第二次有checkpoint【ReliableCheckpointRDD】
        (4) ShuffledRDD[2] at reduceByKey at Spark50_Checkpoint.scala:29 []
         +-(4) MapPartitionsRDD[1] at map at Spark50_Checkpoint.scala:23 []
            |  ReliableCheckpointRDD[3] at foreach at Spark50_Checkpoint.scala:31 []
         */
    }
}

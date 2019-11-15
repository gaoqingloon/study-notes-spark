package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: coalesce 算子
  * version: 1.0
  */
object Spark12_SortBy {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。

        2. 需求：创建一个4个分区的RDD，对其缩减分区
         */
        val conf = new SparkConf()
            .setAppName("coalesce").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        // 缩减分区：可以简单理解为合并分区 1,2,(3,4)
        val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
        println("缩减分区前 = " + listRDD.partitions.length)//4个分区

        // numPartitions: Int, shuffle: Boolean = false,
        // partitionCoalescer: Option[PartitionCoalescer]
        val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
        println("缩减分区后 = " + coalesceRDD.partitions.length)//3个分区

        coalesceRDD.saveAsTextFile("output")

        sc.stop()
    }
}

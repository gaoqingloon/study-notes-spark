package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description:
  * version: 1.0
  */
object Spark01_RDD {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
        // 创建spark上下文对象
        val sc: SparkContext = new SparkContext(conf)


        // 1. 创建RDD
        // 1) 从内存中创建 makeRDD，底层实现就是 parallelize
        val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val result: Array[Int] = listRDD.collect()
        result.foreach(println)

        // 1.1) 使用自定义分区
        // numSlices: Int = defaultParallelism
        // conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
        val listRDDWithPartition: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        listRDDWithPartition.collect().foreach(println)

        // 2) 从内存中创建 parallelize
        val arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))
        arrayRDD.collect().foreach(println)
        arrayRDD.saveAsTextFile("output2")//4

        // 3) 从外部存储中创建
        // 默认情况下，可以读取项目路径，也可以读取其他路径: HDFS
        // 默认从文件中读取的数据都是字符串类型
        val fileRDD: RDD[String] = sc.textFile("data")
        fileRDD.saveAsTextFile("output")//2

        // 3.1) 读取文件：使用自定义分区
        // minPartitions: Int = defaultMinPartitions
        // math.min(defaultParallelism, 2)
        // 读取文件时，传递的分区参数为最小分区数，但是不一定是这个分区数，
        // 取决于hadoop读取文件时分片规则，实际>=最小分区数
        val fileRDDWithPartition: RDD[String] = sc.textFile("data", 1)

        // 2. 将RDD的数据保存到文件中
        fileRDDWithPartition.saveAsTextFile("output1")//1

        sc.stop()
    }
}

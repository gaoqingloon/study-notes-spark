package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: mapPartitions算子
  * version: 1.0
  */
object Spark03_MapPartitions {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，
        func的函数类型必须是Iterator[T] => Iterator[U]。假设有N个元素，有M个分区，
        那么map的函数的将被调用N次,而mapPartitions被调用M次,一个函数一次处理所有分区。
        2. 需求：创建一个RDD，使每个元素*2组成新的RDD
         */
        val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

        // mapPartitions可以对一个RDD中所有的分区进行遍历
        // mapPartitions效率优于map算子，减少了发送到执行器执行的交互次数
        // mapPartitions可能会出现内存溢出（OOM）
        //f: Iterator[T] => Iterator[U]
        val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
            datas.map(data => data * 2)
        })

        mapPartitionsRDD.collect().foreach(println)
        sc.stop()
    }
}

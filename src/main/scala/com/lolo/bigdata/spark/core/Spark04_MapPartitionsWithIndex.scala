package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: mapPartitionsWithIndex算子
  * version: 1.0
  */
object Spark04_MapPartitionsWithIndex {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：类似于mapPartitions，但func带有一个整数参数表示分片的索引值，
            因此在类型为T的RDD上运行时，func的函数类型必须是
            (Int, Iterator[T]) => Iterator[U]；
        2. 需求：创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD
         */
        val conf = new SparkConf()
            .setAppName("mapPartitionsWithIndex").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

        // f: (Int, Iterator[T]) => Iterator[U]
        // 当有多个参数时可以使用模式匹配
        val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
            case (num, datas) =>
                datas.map((_, "分区号: " + num))
                //datas.map(x => (x, "分区号: " + num))
        }
        /*
        (1,分区号: 0)
        (2,分区号: 0)
        (3,分区号: 0)
        (4,分区号: 0)
        (5,分区号: 0)
        (6,分区号: 1)
        (7,分区号: 1)
        (8,分区号: 1)
        (9,分区号: 1)
        (10,分区号: 1)
         */
        tupleRDD.collect().foreach(println)
        sc.stop()
    }
}

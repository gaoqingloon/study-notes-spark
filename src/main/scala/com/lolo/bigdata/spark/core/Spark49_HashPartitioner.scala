package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: Hash分区
  * version: 1.0
  */
object Spark49_HashPartitioner {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("hashPartitioner")
        val sc = new SparkContext(conf)

        val noPar =
            sc.parallelize(List((1, 3), (1, 2), (2, 4), (2, 3), (3, 6), (3, 8)), 8)

        val result: RDD[String] = noPar.mapPartitionsWithIndex {
            case (index, iter) =>
                Iterator(index.toString + " : " + iter.mkString("|"))
        }
        result.collect().foreach(println)
        /*
        0 :
        1 : (1,3)
        2 : (1,2)
        3 : (2,4)
        4 :
        5 : (2,3)
        6 : (3,6)
        7 : (3,8)
         */

        val hashPar = noPar.partitionBy(new org.apache.spark.HashPartitioner(7))
        val count: Long = hashPar.count
        println(count) //6

        val partitioner: Option[Partitioner] = hashPar.partitioner
        partitioner.foreach(println)
        //org.apache.spark.HashPartitioner@7

        val value: RDD[Int] = hashPar.mapPartitions(iter => Iterator(iter.length))
        // 每个分区返回，打印比较乱
        //value.foreach(x => print(x + ", "))

        // 收集回来之后变成一个Array统一打印
        value.collect().foreach(x => print(x + ", "))
        //0, 2, 2, 2, 0, 0, 0,

        sc.stop()
    }
}

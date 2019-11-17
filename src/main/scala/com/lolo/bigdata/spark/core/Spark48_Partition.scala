package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: 获取RDD分区器
  * version: 1.0
  */
object Spark48_Partition {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("checkpoint")
        val sc = new SparkContext(conf)

        val pairs = sc.parallelize(List((1, 1), (2, 2), (3, 3)))
        val partitioner: Option[Partitioner] = pairs.partitioner
        partitioner.foreach(println)
        //None

        //使用HashPartitioner对RDD进行重新分区
        val partitioned: RDD[(Int, Int)] = pairs.partitionBy(new HashPartitioner(2))
        val partitioner1: Option[Partitioner] = partitioned.partitioner
        partitioner1.foreach(println)
        //org.apache.spark.HashPartitioner@2

        sc.stop()
    }
}
/*
def getPartition(key: Any): Int = key match {
  case null => 0
  case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
}
def nonNegativeMod(x: Int, mod: Int): Int = {
  val rawMod = x % mod
  rawMod + (if (rawMod < 0) mod else 0)
}
 */

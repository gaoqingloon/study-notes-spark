package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: sortByKey算子
  * version: 1.0
  */
object Spark26_SortByKey {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：在一个(K,V)的RDD上调用，K必须实现Ordered接口，
            返回一个按照key进行排序的(K,V)的RDD

        2. 需求：创建一个pairRDD，按照key的正序和倒序进行排序
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("sortByKey")
        val sc: SparkContext = new SparkContext(conf)

        val wordPairsRDD: RDD[(Int, String)] =
            sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))


        // ascending: Boolean = true, numPartitions: Int = self.partitions.length
        val sortByKeyAscRDD: RDD[(Int, String)] = wordPairsRDD.sortByKey(ascending = true)
        val resTuple: Array[(Int, String)] = sortByKeyAscRDD.collect()

        resTuple.foreach(x => print(x + "。"))
        println()
        //(1,dd)。(2,bb)。(3,aa)。(6,cc)。


        val sortByKeyDescRDD: RDD[(Int, String)] = wordPairsRDD.sortByKey(ascending = false)
        val resTuple2: Array[(Int, String)] = sortByKeyDescRDD.collect()

        resTuple2.foreach(x => print(x + "。"))
        println()
        //(6,cc)。(3,aa)。(2,bb)。(1,dd)。

        sc.stop()
    }
}

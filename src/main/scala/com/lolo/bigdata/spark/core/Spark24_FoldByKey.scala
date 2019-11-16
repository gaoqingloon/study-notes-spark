package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: foldByKey算子
  * version: 1.0
  */
object Spark24_FoldByKey {

    def main(args: Array[String]): Unit = {

        /*
        1.	作用：aggregateByKey的简化操作，seqop和combop相同

        2.	需求：创建一个pairRDD，计算相同key对应值的相加结果
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("foldByKey")
        val sc: SparkContext = new SparkContext(conf)

        val wordPairsRDD: RDD[(Int, Int)] =
            sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)


        // 柯里化
        // (zeroValue: V)(func: (V, V) => V)
        // (zeroValue: V, numPartitions: Int)(func: (V, V) => V)
        // (zeroValue: V, partitioner: Partitioner)(func: (V, V) => V)
        val foldByKeyRDD: RDD[(Int, Int)] = wordPairsRDD.foldByKey(0)(_ + _)
        val resTuple: Array[(Int, Int)] = foldByKeyRDD.collect()
        resTuple.foreach(x => print(x + "。"))
        println()
        //(3,14)。(1,9)。(2,3)。

        // wordCount
        val aggRDD: RDD[(Int, Int)] = wordPairsRDD.aggregateByKey(0)(_ + _, _ + _)
        aggRDD.collect().foreach(x => print(x + "。"))
        println()
        //(3,14)。(1,9)。(2,3)。

        sc.stop()
    }
}

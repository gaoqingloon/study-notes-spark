package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: groupByKey算子
  * version: 1.0
  */
object Spark21_GroupByKey {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：groupByKey也是对每个key进行操作，但只生成一个sequence。

        2. 需求：创建一个pairRDD，将相同key对应值聚合到一个sequence中，
            并计算相同key对应值的相加结果。
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
        val sc: SparkContext = new SparkContext(conf)

        val words: Array[String] = Array("one", "two", "two", "three", "three", "three")
        val wordPairsRDD: RDD[(String, Int)] = sc.makeRDD(words).map(x => (x, 1))


        // () ==> groupByKey(defaultPartitioner(self))
        // numPartitions: Int
        // partitioner: Partitioner
        val groupByKeyRDD: RDD[(String, Iterable[Int])] = wordPairsRDD.groupByKey()

        val resTuple: Array[(String, Iterable[Int])] = groupByKeyRDD.collect()
        resTuple.foreach(x => print(x + ", "))
        println()
        //(two,CompactBuffer(1, 1)), (one,CompactBuffer(1)), (three,CompactBuffer(1, 1, 1)),

        // 计算相同key对应值的相加结果
        val res: RDD[(String, Int)] = groupByKeyRDD.map(t => (t._1, t._2.sum))
        val tuples: Array[(String, Int)] = res.collect()
        for (elem <- tuples) { print(elem + " ") }
        println()
        //(two,2) (one,1) (three,3)

        sc.stop()
    }
}

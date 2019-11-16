package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: combineByKey算子
  * version: 1.0
  */
object Spark25_CombineByKey {

    def main(args: Array[String]): Unit = {

        /*
        1.	作用：对相同K，把V合并成一个集合

        createCombiner: V => C,
        mergeValue: (C, V) => C,
        mergeCombiners: (C, C) => C

    （1）createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个
        元素的键要么还没有遇到过，要么就和之前的某个元素的键相同。如果这是一个
        新的元素,combineByKey()会使用一个叫作createCombiner()的函数来
        创建那个键对应的累加器的初始值
    （2）mergeValue: 如果这是一个在处理当前分区之前已经遇到的键，它会使用
        mergeValue()方法将该键的累加器对应的当前值与这个新的值进行合并
    （3）mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以
        有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器， 就需要
        使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。

        3.	需求：创建一个pairRDD，根据key计算每种key的均值。
            （先计算每个key出现的次数以及可以对应值的总和，再相除得到结果）
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("combineByKey")
        val sc: SparkContext = new SparkContext(conf)

        val inputRDD: RDD[(String, Int)] =
            sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)


        // 传递3个函数
        // createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C
        /*val combineByKeyRDD: RDD[(String, (Int, Int))] = inputRDD.combineByKey(
            x => (x, 1), // (值,次数1)
            (acc: (Int, Int), v: Int) => (acc._1 + v, acc._2 + 1), // (值+v,次数+1)
            (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
        )*/

        val combineByKeyRDD: RDD[(String, (Int, Int))] = inputRDD.combineByKey(
            (_, 1), // (值,次数1)
            (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), // (值+v,次数+1)
            (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
        )

        val resTuple: Array[(String, (Int, Int))] = combineByKeyRDD.collect()
        // Array((b,(286,3)), (a,(274,3)))
        resTuple.foreach(x => print(x + "。"))
        println()

        /*val result: RDD[(String, Double)] =
            combineByKeyRDD.map(x =>  (x._1, x._2._1/x._2._2.toDouble))*/
        val result: RDD[(String, Double)] = combineByKeyRDD.map {
            case (key, value) => (key, value._1 / value._2.toDouble)
        }

        result.collect().foreach(x => print(x + "。"))
        println()
        //(b,95.33333333333333)。(a,91.33333333333333)。


        /** wordCount */
        val wordPairsRDD: RDD[(Int, Int)] =
            sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)


        val aggRDD: RDD[(Int, Int)] = wordPairsRDD.aggregateByKey(0)(_ + _, _ + _)
        aggRDD.collect().foreach(x => print(x + "。"))
        println()
        //(3,14)。(1,9)。(2,3)。

        val aggRDD1: RDD[(Int, Int)] = wordPairsRDD.foldByKey(0)(_ + _)
        aggRDD1.collect().foreach(x => print(x + "。"))
        println()
        //(3,14)。(1,9)。(2,3)。

        val aggRDD2: RDD[(Int, Int)] = wordPairsRDD.combineByKey(x => x, (x: Int, y) => x + y, _ + _)
        aggRDD2.collect().foreach(x => print(x + "。"))
        println()
        //(3,14)。(1,9)。(2,3)。

        sc.stop()
    }
}

package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: aggregateByKey算子
  * version: 1.0
  */
object Spark23_AggregateByKey {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：在kv对的RDD中，，按key将value进行分组合并，合并时，
            将每个value和初始值作为seq函数的参数，进行计算，返回的结果
            作为一个新的kv对，然后再将结果按照key进行合并，最后将每个
            分组的value传递给combine函数进行计算（先将前两个value
            进行计算，将返回结果和下一个value传给combine函数，以此类推），
            将key与计算结果作为一个新的kv对输出。

        (zeroValue: U, [partitioner: Partitioner])
        (seqOp: (U, V) => U, combOp: (U, U) => U)

        zeroValue：给每一个分区中的每一个key一个初始值；
        seqOp：(分区内)函数用于在每一个分区中用初始值逐步迭代value；
        combOp：(分区间)函数用于合并每个分区中的结果。

        2. 需求：创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey")
        val sc: SparkContext = new SparkContext(conf)

        val wordPairsRDD: RDD[(String, Int)] =
            sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)


        // 柯里化
        // (zeroValue: U, [numPartitions: Int | partitioner: Partitioner])
        // (seqOp: (U, V) => U, combOp: (U, U) => U)
        val aggRDD: RDD[(String, Int)] = wordPairsRDD.aggregateByKey(0)(math.max(_, _), _ + _)
        //val aggRDD: RDD[(String, Int)] = wordPairsRDD.aggregateByKey(0)(math.max, _ + _)

        val resTuple: Array[(String, Int)] = aggRDD.collect()
        resTuple.foreach(x => print(x + "。"))
        println()
        //(b,3)。(a,3)。(c,12)。

        val aggRDD1: RDD[(String, Int)] = wordPairsRDD.aggregateByKey(10)(math.max(_, _), _ + _)
        aggRDD1.collect().foreach(x => print(x + "。"))
        println()
        //(b,10)。(a,10)。(c,20)。

        // wordCount
        val aggRDD2: RDD[(String, Int)] = wordPairsRDD.aggregateByKey(0)(_ + _, _ + _)
        aggRDD2.collect().foreach(x => print(x + "。"))
        println()
        //(b,3)。(a,5)。(c,18)。

        sc.stop()
    }
}

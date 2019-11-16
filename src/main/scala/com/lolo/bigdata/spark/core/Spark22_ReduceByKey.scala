package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: reduceByKey算子
  * version: 1.0
  */
object Spark22_ReduceByKey {

    def main(args: Array[String]): Unit = {

        /*
        1. 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的
            reduce函数，将相同key的值聚合到一起，reduce任务的个数
            可以通过第二个可选的参数来设置。

        2. 需求：创建一个pairRDD，计算相同key对应值的相加结果
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("reduceByKey")
        val sc: SparkContext = new SparkContext(conf)

        val words: Array[String] = Array("one", "two", "two", "three", "three", "three")
        val wordPairsRDD: RDD[(String, Int)] = sc.makeRDD(words).map(x => (x, 1))


        // reduceByKey计算相同key对应值的相加结果
        // func: (V, V) => V
        // func: (V, V) => V, numPartitions: Int
        // partitioner: Partitioner, func: (V, V) => V
        val reduceByKeyRDD: RDD[(String, Int)] = wordPairsRDD.reduceByKey((x, y) => x + y)
        val resTuple: Array[(String, Int)] = reduceByKeyRDD.collect()
        resTuple.foreach(x => print(x + "。"))
        println()
        //(two,2)。(one,1)。(three,3)。

        // groupByKey计算相同key对应值的相加结果
        val res: RDD[(String, Int)] = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum))
        val tuples: Array[(String, Int)] = res.collect()
        for (elem <- tuples) { print(elem + " ") }
        println()
        //(two,2) (one,1) (three,3)

        sc.stop()
    }
}
/*
1. reduceByKey：按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[k,v].
2. groupByKey：按照key进行分组，直接进行shuffle。
3. reduceByKey比groupByKey，建议使用。但是需要注意是否会影响业务逻辑。
 */

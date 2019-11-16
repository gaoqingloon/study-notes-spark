package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: cogroup算子(性能低)
  * version: 1.0
  */
object Spark29_Cogroup {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：在类型为(K,V)和(K,W)的RDD上调用，返回
            一个(K,(Iterable<V>,Iterable<W>))类型的RDD

        2. 需求：创建两个pairRDD，并将key相同的数据聚合到一个【迭代器】。
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("cogroup")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
        val rdd2: RDD[(Int, Int)] = sc.parallelize(Array((2, 4), (3, 5), (4, 6)))

        // other: RDD[(K, W)] [Partitioner][numPartitions]
        val cogroupRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd1.cogroup(rdd2)
        val resTuple: Array[(Int, (Iterable[String], Iterable[Int]))] = cogroupRDD.collect()
        resTuple.foreach(x => println(x + "。"))
        // (4,(CompactBuffer(),CompactBuffer(6)))。
        // (1,(CompactBuffer(a),CompactBuffer()))。
        // (2,(CompactBuffer(b),CompactBuffer(4)))。
        // (3,(CompactBuffer(c),CompactBuffer(5)))。

        val cogroupRDD2: RDD[(Int, (Iterable[Int], Iterable[String]))] = rdd2.cogroup(rdd1)
        val resTuple2: Array[(Int, (Iterable[Int], Iterable[String]))] = cogroupRDD2.collect()
        resTuple2.foreach(x => println(x + "。"))
        // (4,(CompactBuffer(6),CompactBuffer()))。
        // (1,(CompactBuffer(),CompactBuffer(a)))。
        // (2,(CompactBuffer(4),CompactBuffer(b)))。
        // (3,(CompactBuffer(5),CompactBuffer(c)))。


        val joinRDD: RDD[(Int, (String, Int))] = rdd1.join(rdd2)
        joinRDD.collect().foreach(x => println(x + "。"))
        // (2,(b,4))。
        // (3,(c,5))。

        val joinRDD2: RDD[(Int, (Int, String))] = rdd2.join(rdd1)
        joinRDD2.collect().foreach(x => println(x + "。"))
        // (2,(4,b))。
        // (3,(5,c))。

        sc.stop()
    }
}

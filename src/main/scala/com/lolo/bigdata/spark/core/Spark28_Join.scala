package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: join算子(性能低)
  * version: 1.0
  */
object Spark28_Join {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个相同
            key对应的所有元素对在一起的(K,(V,W))的RDD

        2. 需求：创建两个pairRDD，并将key相同的数据聚合到一个【元组】。
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("join")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
        val rdd2: RDD[(Int, Int)] = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))

        // other: RDD[(K, W)]
        val joinRDD: RDD[(Int, (String, Int))] = rdd1.join(rdd2)
        val resTuple: Array[(Int, (String, Int))] = joinRDD.collect()
        resTuple.foreach(x => print(x + "。"))
        println()
        //(1,(a,4))。(2,(b,5))。(3,(c,6))。


        val rdd3: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc")))

        val joinRDD2: RDD[(Int, ((String, Int), String))] = joinRDD.join(rdd3)
        val resTuple2: Array[(Int, ((String, Int), String))] = joinRDD2.collect()
        resTuple2.foreach(x => print(x + "。"))
        println()
        //(1,((a,4),aaa))。(2,((b,5),bbb))。(3,((c,6),ccc))。

        sc.stop()
    }
}

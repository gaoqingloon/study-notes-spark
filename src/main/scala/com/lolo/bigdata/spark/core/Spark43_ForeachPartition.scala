package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: foreachPartition算子
  * version: 1.0
  */
object Spark43_ForeachPartition {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("foreachPartition")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 5, 2)

        // f: T => Unit
        // ===> sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
        //val result: Unit = rdd1.foreach(println(_))
        rdd1.foreach(x => print(x + "。"))
        // 3。4。5。1。2。

        // f: Iterator[T] => Unit
        // ===> sc.runJob(this, (iter: Iterator[T]) => cleanF(iter))
        rdd1.foreachPartition(data => {
            data.foreach(x => print(x + "。"))
        })
        // 3。4。5。
        // 1。2。

        sc.stop()
    }
}

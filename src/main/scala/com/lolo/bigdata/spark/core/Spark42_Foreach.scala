package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: foreach算子
  * version: 1.0
  */
object Spark42_Foreach {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：在数据集的每一个元素上，运行函数func进行更新。

        2. 需求：创建一个RDD，对每个元素进行打印
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("foreach")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 5, 2)

        // f: T => Unit
        // ===> sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
        //val result: Unit = rdd1.foreach(println(_))
        rdd1.foreach(x => print(x + "。"))
        // 3。4。5。1。2。
        // 对每个分区进行打印，是一个行动算子，不同于scala的foreach，
        // 执行位置不同，scala==>driver，rdd.foreach==>Executor

        sc.stop()
    }
}

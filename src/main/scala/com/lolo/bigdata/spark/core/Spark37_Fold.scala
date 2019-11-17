package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: fold算子
  * version: 1.0
  */
object Spark37_Fold {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：折叠操作，aggregate的简化操作，seqop和combop一样。

        2. 需求：创建一个RDD，将所有元素相加得到结果
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("fold")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2)

        // 分区内和分区间都会使用初始值
        // (zeroValue: T)(op: (T, T) => T)
        val result: Int = rdd1.fold(0)(_ + _)
        println(result) //55

        val result2: Int = rdd1.fold(10)(_ + _)
        println(result2) //85

        sc.stop()
    }
}

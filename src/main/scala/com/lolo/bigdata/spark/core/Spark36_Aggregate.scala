package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: aggregate算子
  * version: 1.0
  */
object Spark36_Aggregate {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，
        然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
        这个函数最终返回的类型不需要和RDD中元素类型一致。

        2. 需求：创建一个RDD，将所有元素相加得到结果
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("aggregate")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2)

        // 分区内和分区间都会使用初始值
        //(zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)
        val result: Int = rdd1.aggregate(0)(_ + _, _ + _)
        println(result) //55

        val result2: Int = rdd1.aggregate(10)(_ + _, _ + _)
        println(result2) //85

        sc.stop()
    }
}

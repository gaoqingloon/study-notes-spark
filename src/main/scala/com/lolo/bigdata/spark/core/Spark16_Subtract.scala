package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: subtract算子
  * version: 1.0
  */
object Spark16_Subtract {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：计算差的一种函数，去除两个RDD中相同的元素，不同的RDD将保留下来

        2. 需求：创建两个RDD，求第一个RDD与第二个RDD的差集
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("subtract")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD1: RDD[Int] = sc.makeRDD(3 to 8)
        val listRDD2: RDD[Int] = sc.makeRDD(1 to 5)

        // other: RDD[T]
        val subtractRDD: RDD[Int] = listRDD1.subtract(listRDD2)
        val resArray: Array[Int] = subtractRDD.collect()
        resArray.foreach(x => print(x + ", "))
        //subtractRDD.collect().foreach(x => print(x + ", "))
        println()
        //8, 6, 7,

        sc.stop()
    }
}

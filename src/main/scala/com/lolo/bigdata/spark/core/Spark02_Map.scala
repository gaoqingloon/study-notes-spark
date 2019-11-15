package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: map算子
  * version: 1.0
  */
object Spark02_Map {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
        2. 需求：创建一个1-10数组的RDD，将所有元素*2形成新的RDD
         */
        val conf = new SparkConf().setAppName("map").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(1 to 4)
        // 10条数据发给 Executor 10次，进行计算
        // 所有RDD算子的计算功能都由Executor执行
        val mapRDD: RDD[Int] = listRDD.map(x => x * 2)
        //val mapRDD: RDD[Int] = listRDD.map(_ * 2)
        mapRDD.collect().foreach(println)
        sc.stop()
    }
}

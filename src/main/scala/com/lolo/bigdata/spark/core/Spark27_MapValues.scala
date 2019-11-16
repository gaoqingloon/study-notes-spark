package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: mapValues算子
  * version: 1.0
  */
object Spark27_MapValues {

    def main(args: Array[String]): Unit = {

        /*
        1. 针对于(K,V)形式的类型只对V进行操作

        2. 需求：创建一个pairRDD，并将value添加字符串"|||"
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("mapValues")
        val sc: SparkContext = new SparkContext(conf)

        val wordPairsRDD: RDD[(Int, String)] =
            sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))


        // f: V => U
        //val mapValuesRDD: RDD[(Int, String)] = wordPairsRDD.mapValues(x => x + "|||")
        val mapValuesRDD: RDD[(Int, String)] = wordPairsRDD.mapValues(_ + "|||")
        val resTuple: Array[(Int, String)] = mapValuesRDD.collect()

        resTuple.foreach(x => print(x + "。"))
        println()
        //(3,aa|||)。(6,cc|||)。(2,bb|||)。(1,dd|||)。

        sc.stop()
    }
}

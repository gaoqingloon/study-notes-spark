package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: filter算子
  * version: 1.0
  */
object Spark08_Filter {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：过滤。返回一个新的RDD，该RDD由经过func函数
            计算后返回值为true的输入元素组成。
        2. 需求：创建一个RDD（由字符串组成），过滤出一个新RDD（包含”xiao”子串）
         */
        val conf = new SparkConf()
            .setAppName("filter").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[String] = sc.makeRDD(Array("xiaoming","xiaojiang","xiaohe","dazhi"))

        // f: T => Boolean
        val filterRDD: RDD[String] = listRDD.filter(x => x.contains("xiao"))
        //val filterRDD: RDD[String] = listRDD.filter(_.contains("xiao"))

        filterRDD.collect().foreach(println)

        /*
        xiaoming
        xiaojiang
        xiaohe
         */
        sc.stop()
    }
}

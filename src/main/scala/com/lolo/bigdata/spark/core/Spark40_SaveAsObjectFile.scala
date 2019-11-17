package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: saveAsObjectFile算子
  * version: 1.0
  */
object Spark40_SaveAsObjectFile {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：用于将RDD中的元素序列化成对象，存储到文件中。
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("saveAsObjectFile")
        val sc: SparkContext = new SparkContext(conf)

        //val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2) //可以
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

        // path: String, codec: Class[_ <: CompressionCodec] =  压缩格式
        rdd1.saveAsObjectFile("output3")

        sc.stop()
    }
}

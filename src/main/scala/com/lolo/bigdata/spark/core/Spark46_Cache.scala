package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: cache RDD缓存（内存）
  * version: 1.0
  */
object Spark46_Cache {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("")
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[String] = sc.makeRDD(Array("spark"))

        val noCache: RDD[String] = rdd.map(_.toString + System.currentTimeMillis)
        noCache.foreach(println) //spark1573988356063
        noCache.foreach(println) //spark1573988356199
        noCache.foreach(println) //spark1573988356332

        val value: RDD[String] = rdd.map(_.toString + System.currentTimeMillis)
        val cache: RDD[String] = value.cache //进行缓存
        cache.foreach(println) //spark1573988482420
        cache.foreach(println) //spark1573988482420
        cache.foreach(println) //spark1573988482420

        sc.stop()
    }
}

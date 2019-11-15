package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: WordCount案例
  * version: 1.0
  */
object WordCount {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc: SparkContext = new SparkContext(conf)

        val WordAndSum: RDD[(String, Int)] = sc.textFile(args(0))
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
        WordAndSum.collect().foreach(println)

        sc.stop()
    }
}

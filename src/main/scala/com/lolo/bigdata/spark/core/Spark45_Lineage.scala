package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: RDD依赖关系
  * version: 1.0
  */
object Spark45_Lineage {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("lineage")
        val sc: SparkContext = new SparkContext(conf)

        val wordAndOne: RDD[(String, Int)] =
            sc.textFile("data/fruit.csv")
                .flatMap(_.split(","))
                .map((_, 1))

        val wordAndCount = wordAndOne.reduceByKey(_ + _)
        wordAndCount.foreach(println)

        // 1. 查看Lineage
        println(wordAndOne.toDebugString)
        /*
        (2) MapPartitionsRDD[3] at map at Spark45_Lineage.scala:22 []
         |  MapPartitionsRDD[2] at flatMap at Spark45_Lineage.scala:21 []
         |  data/fruit.csv MapPartitionsRDD[1] at textFile at Spark45_Lineage.scala:20 []
         |  data/fruit.csv HadoopRDD[0] at textFile at Spark45_Lineage.scala:20 []
         */

        println(wordAndCount.toDebugString)
        /*
        (2) ShuffledRDD[4] at reduceByKey at Spark45_Lineage.scala:24 []
         +-(2) MapPartitionsRDD[3] at map at Spark45_Lineage.scala:22 []
            |  MapPartitionsRDD[2] at flatMap at Spark45_Lineage.scala:21 []
            |  data/fruit.csv MapPartitionsRDD[1] at textFile at Spark45_Lineage.scala:20 []
            |  data/fruit.csv HadoopRDD[0] at textFile at Spark45_Lineage.scala:20 []
         */

        // 2. 查看依赖类型
        println(wordAndOne.dependencies)
        // List(org.apache.spark.OneToOneDependency@b7a6492)

        println(wordAndCount.dependencies)
        // List(org.apache.spark.ShuffleDependency@465ac330)

        sc.stop()
    }
}

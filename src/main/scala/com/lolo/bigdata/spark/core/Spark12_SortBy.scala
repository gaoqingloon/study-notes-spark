package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: sortBy 算子
  * version: 1.0
  */
object Spark12_SortBy {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用；使用func先对数据进行处理，按照处理后的数据比较结果排序，默认为正序。

        2. 需求：创建一个RDD，按照不同的规则进行排序
         */
        val conf = new SparkConf()
            .setAppName("sortBy").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(List(2, 1, 3, 4))

        // f: (T) => K, ascending: Boolean = true,
        // numPartitions: Int = this.partitions.length
        val sortByRDD: RDD[Int] = listRDD.sortBy(x => x, ascending = false)
        sortByRDD.collect().foreach(x => print(x  + " "))
        println()

        sc.stop()
    }
}

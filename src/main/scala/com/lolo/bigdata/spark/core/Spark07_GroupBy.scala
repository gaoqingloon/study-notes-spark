package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: groupBy算子
  * version: 1.0
  */
object Spark07_GroupBy {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：分组，按照传入函数的返回值进行分组。
            将相同的key对应的值放入一个迭代器。
        2. 需求：创建一个RDD，按照元素模以2的值进行分组。
         */
        val conf = new SparkConf()
            .setAppName("groupBy").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(1 to 4)

        // 生成数据，按照指定的规则进行分组
        // 分组后的数据形成了对偶元组(K-V)，K表示分组的key，v表示分组的数据集合
        // f: T => K,
        val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i % 2)

        groupByRDD.collect().foreach(println)

        /*
        (0,CompactBuffer(2, 4))
        (1,CompactBuffer(1, 3))
         */
        sc.stop()
    }
}

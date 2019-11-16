package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: zip算子
  * version: 1.0
  */
object Spark19_Zip {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的
            partition数量以及元素数量都相同，否则会抛出异常。

        2. 需求：创建两个RDD，并将两个RDD组合到一起形成一个(k,v)RDD
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("zip")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD1: RDD[Int] = sc.makeRDD(Array(1, 2, 3), 3)
        val listRDD2: RDD[String] = sc.makeRDD(Array("a","b","c"), 3)
        //val listRDD2: RDD[String] = sc.makeRDD(Array("a","b","c"), 4)//错误
        //val listRDD2: RDD[String] = sc.makeRDD(Array("a","b","c"), 2)//错误
        // java.lang.IllegalArgumentException: Can't zip RDDs with unequal
        // numbers of partitions: List(3, 2)

        // other: RDD[U]
        val zipRDD: RDD[(Int, String)] = listRDD1.zip(listRDD2)
        val resTupleArray: Array[(Int, String)] = zipRDD.collect()

        resTupleArray.foreach(x => print(x + ", "))
        //resTupleArray.collect().foreach(x => print(x + ", "))
        println()
        //(1,a), (2,b), (3,c),

        sc.stop()
    }
}

package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: serializable序列化问题
  * version: 1.0
  */
object Spark44_Serializable {

    def main(args: Array[String]): Unit = {

        //1.初始化配置信息及SparkContext
        val conf = new SparkConf().setMaster("local[*]").setAppName("serializable")
        val sc: SparkContext = new SparkContext(conf)

        //2.创建一个RDD
        val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "hbase"))

        //3.创建一个Search对象
        val search = new Search("h")

        //4.运用第一个过滤函数并打印结果
        //val match1: RDD[String] = search.getMatch1(rdd)
        //match1.collect().foreach(println)

        val match2: RDD[String] = search.getMatch2(rdd)
        match2.collect().foreach(println)

        sc.stop()
    }
}

/**
  * 若Executor端使用类中的某些属性，需要将类序列化
  * 关键要掌握具体哪些代码的运行位置Driver端还是Executor端
  */
class Search(query: String) {
//java.io.NotSerializableException: com.lolo.bigdata.spark.core.Search
//class Search(query: String) extends Serializable {

    //过滤出包含字符串的数据
    def isMatch(s: String): Boolean = {
        s.contains(query)
    }

    //过滤出包含字符串的RDD
    def getMatch1(rdd: RDD[String]): RDD[String] = {
        rdd.filter(isMatch)
    }

    //过滤出包含字符串的RDD
    def getMatch2(rdd: RDD[String]): RDD[String] = {
        //将类变量赋值给局部变量，不需要将类序列化
        val s = this.query
        rdd.filter(x => x.contains(s))
        //query为全局变量，需要将类序列化
        //rdd.filter(x => x.contains(query))
    }
}

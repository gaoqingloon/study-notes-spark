package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description:
  * version: 1.0
  */
object Spark50_JSON {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("json")
        val sc = new SparkContext(conf)

        val json: RDD[String] = sc.textFile("data/user.json")
        val result: RDD[Option[Any]] = json.map(JSON.parseFull)

        result.foreach(println)

        /*
        Some(Map(name -> 789, age -> 20.0))
        Some(Map(name -> 123, age -> 18.0))
        Some(Map(name -> 456, age -> 19.0))
         */
    }
}

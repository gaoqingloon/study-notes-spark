package com.lolo.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: DSL（领域特定语言）
  * version: 1.0
  */
object SparkSQL01_DSL {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("dsl").setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

        // 读取数据，构建DataFrame
        val df: DataFrame = spark.read.json("data/user.json")

        // 1. 使用DSL语言进行查询
        df.select("name").show()
        df.show()

        /*
        +------+
        |  name|
        +------+
        |gordon|
        |  tony|
        |   tom|
        +------+
        +---+------+
        |age|  name|
        +---+------+
        | 18|gordon|
        | 19|  tony|
        | 20|   tom|
        +---+------+
         */

        spark.stop()
    }
}

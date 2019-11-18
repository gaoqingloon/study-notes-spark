package com.lolo.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: SQL语法访问数据
  * version: 1.0
  */
object SparkSQL02_SQL {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("sql").setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

        // 读取数据，构建DataFrame
        val df: DataFrame = spark.read.json("data/user.json")

        // 1. 将DataFrame转换为一张表
        df.createOrReplaceTempView("user")

        // 2. 采用sql的语法访问数据
        val frame: DataFrame = spark.sql("select * from user")
        frame.show()
        /*
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

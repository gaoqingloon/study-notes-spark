package com.lolo.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: User Define Function
  * version: 1.0
  */
object SparkSQL04_UDF {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("udf").setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()


        val userDF: DataFrame = spark.read.json("data/user.json")
        userDF.show()
        /*
        +---+------+
        |age|  name|
        +---+------+
        | 18|gordon|
        | 19|  tony|
        | 20|   tom|
        +---+------+
         */

        // 自定义udf函数
        val addA = (x: String) => {
            "A: " + x
        }

        // 1. 注册udf函数
        spark.udf.register("addName", (x: String) => "Name:" + x)
        spark.udf.register("addA", addA)

        // 2. 使用udf函数查询
        userDF.createOrReplaceTempView("user")
        spark.sql("select addName(name),age from user").show()
        /*
        +-----------------+---+
        |UDF:addName(name)|age|
        +-----------------+---+
        |      Name:gordon| 18|
        |        Name:tony| 19|
        |         Name:tom| 20|
        +-----------------+---+
         */

        spark.stop()
    }
}

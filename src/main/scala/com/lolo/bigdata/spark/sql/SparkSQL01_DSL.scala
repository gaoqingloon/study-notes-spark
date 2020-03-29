package com.lolo.bigdata.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: DSL（领域特定语言）
  * version: 1.0
  */
object SparkSQL01_DSL {

    def main(args: Array[String]): Unit = {

        /* // 创建SparkConf()并设置App名称
        val spark = SparkSession
            .builder()
            .appName("Spark SQL basic example")
            .master("local[4]")
            .getOrCreate()*/

        val conf = new SparkConf().setAppName("dsl").setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

        val sc: SparkContext = spark.sparkContext
        println(sc)

        // For implicit conversions like converting RDDs to DataFrames
        // 通过引入隐式转换可以将RDD的操作添加到 DataFrame上
        import spark.implicits._

        // 通过spark.read操作读取JSON数据，构建DataFrame
        val df: DataFrame = spark.read.json("data/user.json")
        //val df: DataFrame = spark.read.format("json").load("data/user.json")

        // Dataset对RDD的进一步封装
        val lines: Dataset[String] = spark.read.textFile("data/word.txt")
        val words: Dataset[String] = lines.flatMap(_.split(" "))
        words.createOrReplaceTempView("wc")
        // 提示group by的字段不能是别名，需要是value
        val result: DataFrame = spark.sql("SELECT value word, COUNT(*) counts FROM wc GROUP BY value ORDER BY counts DESC")
        result.show()

        /*  //只有一列，默认叫value
        spark.sql("SELECT * FROM wc").show()
        +-----+
        |value|
        +-----+
        |hello|
        |world|
        |hello|
        |spark|
        |hello|
        |scala|
        +-----+
        +-----+------+
        | word|counts|
        +-----+------+
        |hello|     3|
        |world|     1|
        |scala|     1|
        |spark|     1|
        +-----+------+
         */

        // 1. 使用DSL语言进行查询
        // Displays the content of the DataFrame to console
        // show操作类似于Action，将DataFrame直接打印到Console
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
        // 2. DSL风格的使用方式中，属性的获取方法
        // 需要隐式转换 import spark.implicits._
        val res: Dataset[Row] = df.filter($"age" > 21)
        res.show()
        //df.where($"fv" > 98).orderBy($"fv" desc, $"age" asc)

        // 关闭整个SparkSession
        spark.stop()
    }
}

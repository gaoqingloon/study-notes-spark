package com.lolo.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description:
  * version: 1.0
  */
object SparkSQL03_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("transform").setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

        // RDD 与 DF/DS 之间转换需要使用隐式转换
        import spark.implicits._ //spark是SparkSession名字，不是包名

        // 1. RDD -> DF -> DS -> DF -> RDD
        transform(spark)

        // 2. RDD -> Dataset
        val rdd: RDD[(Int, String, Int)] =
            spark.sparkContext.makeRDD(List((1, "zhangsan", 20),(2, "lisi", 30),(3, "wangwu", 40)))

        val userRDD: RDD[User] = rdd.map {
            case (id, name, age) => {
                User(id, name, age)
            }
        }
        val userDS: Dataset[User] = userRDD.toDS()

        // 3. Dataset -> RDD
        val rdd1: RDD[User] = userDS.rdd

        rdd1.foreach(println)
        /*
        User(3,wangwu,40)
        User(2,lisi,30)
        User(1,zhangsan,20)
         */

        spark.stop()
    }

    def transform(spark: SparkSession): Unit = {

        // RDD 与 DF/DS 之间转换需要使用隐式转换
        import spark.implicits._ //spark是SparkSession名字，不是包名

        // 1. 创建RDD
        val rdd: RDD[(Int, String, Int)] =
            spark.sparkContext.makeRDD(List((1, "zhangsan", 20),(2, "lisi", 30),(3, "wangwu", 40)))

        // 2. 转换为DF
        val df: DataFrame = rdd.toDF("id", "name", "age")

        // 3. 转换为DS
        val ds: Dataset[User] = df.as[User]

        // 4. 转换为DF
        val df1: DataFrame = ds.toDF()

        // 5. 转换为RDD
        val rdd1: RDD[Row] = df1.rdd

        rdd1.foreach(row => {
            // 获取数据时，可以通过索引访问数据
            println(row.getString(1))
        })
        /*
        wangwu
        zhangsan
        lisi
         */
    }
}

case class User(id: Int, name: String, age: Int)

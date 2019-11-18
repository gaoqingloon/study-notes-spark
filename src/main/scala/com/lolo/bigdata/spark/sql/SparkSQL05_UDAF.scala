package com.lolo.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: User Define Aggregate Function(弱类型)
  * version: 1.0
  */
object SparkSQL05_UDAF {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("udaf").setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

        val df: DataFrame = spark.read.json("data/user.json")
        df.show()
        /*
        +---+------+
        |age|  name|
        +---+------+
        | 18|gordon|
        | 19|  tony|
        | 20|   tom|
        +---+------+
         */

        // 1. 创建并注册聚合函数
        val udaf: MyAgeAvgFunction = new MyAgeAvgFunction()
        spark.udf.register("avgAge", udaf)

        // 2. 使用udaf函数查询
        df.createOrReplaceTempView("user")
        spark.sql("select avgAge(age) as avg_age from user").show()
        /*
        +---------------------+
        |myageavgfunction(age)|
        +---------------------+
        |                 19.0|
        +---------------------+
        +-------+
        |avg_age|
        +-------+
        |   19.0|
        +-------+
         */

        spark.stop()
    }
}

/**
  * 声明用户自定义聚合函数：年龄的平均值=年龄总和/个数
  * 输入：age   中间计算: sum count   输出：avg_age
  * 1) 继承UserDefinedAggregateFunction
  * 2) 实现方法(输入，实现逻辑，返回)
  */
class MyAgeAvgFunction extends UserDefinedAggregateFunction {

    // 【输入结构】函数输入的数据结构
    override def inputSchema: StructType = {
        new StructType().add("age", LongType)
    }

    // 【缓冲结构】计算时的数据结构
    override def bufferSchema: StructType = {
        new StructType().add("sum", LongType).add("count", LongType)
    }

    // 【返回类型】函数返回的数据类型
    override def dataType: DataType = {
        DoubleType
    }

    // 函数是否稳定（给相同的输入是否相同的输出）
    override def deterministic: Boolean = {
        true
    }

    // 【初始化】计算之前的缓冲区的初始化（sum和count的初始值）
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L //sum
        buffer(1) = 0L //count
    }

    // 【更新】根据查询结果更新缓冲区数据，input表示输入的数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getLong(0) + input.getLong(0) //sum
        buffer(1) = buffer.getLong(1) + 1 //count
    }

    // 【合并】将多个节点的缓冲区合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0) //sum
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1) //count
    }

    // 【计算】计算最终的结果
    override def evaluate(buffer: Row): Any = {
        buffer.getLong(0) / buffer.getLong(1).toDouble
    }
}

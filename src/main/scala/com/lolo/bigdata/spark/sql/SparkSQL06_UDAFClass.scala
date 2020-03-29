package com.lolo.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: User Define Aggregate Function(强类型)
  * version: 1.0
  */
object SparkSQL06_UDAFClass {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("udaf").setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

        // RDD 与 DF/DS 之间转换需要使用隐式转换
        import spark.implicits._ //spark是SparkSession名字，不是包名


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
        val udaf: MyAgeAvgClassFunction = new MyAgeAvgClassFunction()

        // 2. 将聚合函数转换为查询的列
        val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")

        // 3. 使用udaf函数查询
        val userDS: Dataset[UserBean] = df.as[UserBean]
        userDS.select(avgCol).show()
        /*
        +------+
        |avgAge|
        +------+
        |  19.0|
        +------+
         */

        spark.stop()
    }
}

// 既然是强类型，可能有case类
case class UserBean(name: String, age: BigInt)

// 样例类属性默认为val，不可以修改; 可以使用Long类型
case class AvgBuffer(var sum: BigInt, var count: Int)

/**
  * 声明用户自定义聚合函数（强类型UDAF函数【DSL使用】）
  * 1) 继承Aggregator，设定泛型
  * 2) 实现方法(输入，实现逻辑，返回)
  */
class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {

    // 初始化缓冲区
    override def zero: AvgBuffer = {
        AvgBuffer(0, 0)
    }

    // 聚合数据：输入数据与当前缓冲区做处理
    override def reduce(buffer: AvgBuffer, user: UserBean): AvgBuffer = {
        buffer.sum += user.age
        buffer.count += 1
        buffer
    }

    // 缓冲区的合并操作(聚合不同execute)
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
        b1.sum += b2.sum
        b1.count += b2.count
        b1
    }

    // 完成计算
    override def finish(reduction: AvgBuffer): Double = {
        reduction.sum.toDouble / reduction.count
    }

    // 设定中间值类型的编码器，要转换成case类：自定义 Encoders.product
    // Encoders.product是进行scala元组和case类转换的编码器
    override def bufferEncoder: Encoder[AvgBuffer] = {
        Encoders.product
    }

    // 设定最终输出值的编码器：系统 Encoders.product
    override def outputEncoder: Encoder[Double] = {
        Encoders.scalaDouble
    }
}

class MyAveTest extends Aggregator[UserBean, AvgBuffer, Double] {

    // 用于定义一个聚合函数内部需要的数据结构
    override def zero: AvgBuffer = ???

    // 针对每个分区内部每一个输入来更新你的数据结构
    override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = ???

    // 用于对于不同分区的结构进行聚合
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = ???

    // 计算输出
    override def finish(reduction: AvgBuffer): Double = ???

    // 用于数据结构他的转换
    override def bufferEncoder: Encoder[AvgBuffer] = ???

    // 用于最终结果的转换
    override def outputEncoder: Encoder[Double] = ???
}

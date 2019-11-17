package com.lolo.bigdata.spark.core

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: 累加器（数据增多）
  * version: 1.0
  */
object Spark51_Accumulator {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
        val sc = new SparkContext(conf)

        // SystemAccumulator(sc)
        CustomAccumulator(sc)

        sc.stop()
    }

    /**
      * 使用系统累加器累加数据
      */
    def SystemAccumulator(sc: SparkContext): Unit = {

        val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        // 方式1：普通的reduce方式
        val sum1: Int = dataRDD.reduce(_ + _)
        println(sum1) //10

        // 方式2：错误的方式
        var sum2: Int = 0
        dataRDD.foreach(x => sum2 += x)
        // 因为两个分区，Executor端累加完之后没有进行返回
        println(sum2) //0

        // 方式2：使用累加器
        // 1) 创建累加器对象
        val accumulator: LongAccumulator = sc.longAccumulator
        dataRDD.foreach {
            case x => {
                // 2) 执行累加器的累加功能
                accumulator.add(x)
            }
        }
        // 3) 获取累加器的值
        println(accumulator.value)
    }

    /**
      * 自定义累加器测试
      */
    def CustomAccumulator(sc: SparkContext): Unit = {

        val dataRDD: RDD[String] =
            sc.makeRDD(List("Hadoop", "hive", "HBase", "Scala", "Spark"), 2)

        // 1) 创建自定义累加器
        val wordAccumulator: WordAccumulator = new WordAccumulator

        // 2) 注册累加器
        sc.register(wordAccumulator)

        dataRDD.foreach {
            case word => {
                //执行累加器的累加功能
                wordAccumulator.add(word)
            }
        }
        // 获取累加器的值
        println(wordAccumulator.value)
        //[Hadoop, hive, HBase]
    }
}

/**
  * 自定义累加器
  * 1) 声明累加器，继承AccumulatorV2[输入,输出]
  * 2) 实现抽象方式
  * 3) 创建累加器
  */
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

    private val list = new util.ArrayList[String]()

    // 当前的累加器是否为初始化状态，根据声明的list是否为空
    override def isZero: Boolean = {
        list.isEmpty
    }

    // 复制累加器对象
    override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
        new WordAccumulator
    }

    // 重置累加器对象
    override def reset(): Unit = {
        list.clear()
    }

    // 向累加器中增加数据
    override def add(v: String): Unit = {
        if (v.toLowerCase.contains("h")) {
            list.add(v)
        }
    }

    // 合并累加器（集合数据的更新）
    override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
        list.addAll(other.value)
    }

    // 获取累加器的结果
    override def value: util.ArrayList[String] = {
        list
    }
}

package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: saveAsTextFile算子
  * version: 1.0
  */
object Spark38_SaveAsTextFile {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：将数据集的元素以textfile的形式保存到HDFS文件系统或者
            其他支持的文件系统，对于每个元素，Spark将会调用toString方法，
            将它装换为文件中的文本
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("saveAsTextFile")
        val sc: SparkContext = new SparkContext(conf)

        //val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2) //可以
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

        // path: String
        // path: String, codec: Class[_ <: CompressionCodec] 压缩格式
        rdd1.saveAsTextFile("output1")

        sc.stop()
    }
}

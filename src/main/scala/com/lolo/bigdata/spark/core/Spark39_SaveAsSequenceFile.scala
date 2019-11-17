package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: saveAsSequenceFile算子
  * version: 1.0
  */
object Spark39_SaveAsSequenceFile {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：将数据集中的元素以Hadoop sequencefile的格式保存
            到指定的目录下，可以使HDFS或者其他Hadoop支持的文件系统。
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("saveAsSequenceFile")
        val sc: SparkContext = new SparkContext(conf)

        //val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2) //这种类型不行
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

        // path: String, codec: Class[_ <: CompressionCodec] =  压缩格式
        rdd1.saveAsSequenceFile("output2")

        sc.stop()
    }
}

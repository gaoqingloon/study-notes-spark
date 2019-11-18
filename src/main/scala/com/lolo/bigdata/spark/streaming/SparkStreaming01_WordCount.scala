package com.lolo.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: SparkStreaming完成wordCount
  * version: 1.0
  */
object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // spark配置对象
        val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")

        // 实时数据分析环境对象，采集周期：以指定的时间为周期采集实时数据
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

        // 1. 从指定的端口中采集数据
        val socketLineDStream: ReceiverInputDStream[String] =
            ssc.socketTextStream("hadoop102", 9999)

        // 将采集的数据进行分解（扁平化）
        val wordDStream: DStream[String] = socketLineDStream.flatMap(line => line.split(" "))

        // 将数据进行结构的转换方便统计分析
        val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

        // 将转换结构后的数据进行聚合处理
        val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

        // 将结果打印出来
        wordToSumDStream.print()

        // 不能停止采集程序
        //ssc.stop()

        // main程序必须和采集器关联，同时停止/开始
        // 启动采集器
        ssc.start()
        // Driver等待采集器执行
        ssc.awaitTermination()
    }
}

package com.lolo.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: 设置窗口  RDD算子中的逻辑一定在Executor中执行
  * version: 1.0
  */
object SparkStreaming06_Window {

    def main(args: Array[String]): Unit = {

        // 1. scala中的窗口
        //scalaWindow()

        // 2. sparkStreaming中的窗口
        val conf = new SparkConf().setMaster("local[*]").setAppName("window")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

        // 保存数据的状态，需要设定检查点的路径
        ssc.sparkContext.setCheckpointDir("checkpoint")

        // 从kafka中采集数据
        val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
            ssc,
            "hadoop102:2181,hadoop103:2181,hadoop104:2181", //zookeeper
            "gordon", //groupId
            Map("gordon" -> 3) //topics
        )

        // 窗口大小应该为采集周期的整数倍，窗口滑动的步长也应该为采集周期的整数倍
        // windowDuration: Duration, slideDuration: Duration
        val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9), Seconds(3))

        // 将采集的数据进行分解（扁平化）
        val wordDStream: DStream[String] = windowDStream.flatMap(t => t._2.split(" "))

        // 将数据进行结构的转换方便统计分析
        val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

        // 将转换结构后的数据进行聚合处理
        //val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
        val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
            case (seq, buffer) => {
                val sum = buffer.getOrElse(0) + seq.sum
                Option(sum)
            }
        }

        // 将结果打印出来
        stateDStream.print()

        // 不能停止采集程序
        //ssc.stop()

        // main程序必须和采集器关联，同时停止/开始
        // 启动采集器
        ssc.start()
        // Driver等待采集器执行
        ssc.awaitTermination()
    }

    /**
      * scala 窗口
      */
    def scalaWindow(): Unit = {

        val lists: List[Int] = List(1, 2, 3, 4, 5)

        // 滑动窗口函数
        // size: Int, step: Int
        val slidingList: Iterator[List[Int]] = lists.sliding(2, 1)

        for (list <- slidingList) {
            println(list.mkString(","))
        }
        /*
        1,2
        2,3
        3,4
        4,5
         */
    }
}

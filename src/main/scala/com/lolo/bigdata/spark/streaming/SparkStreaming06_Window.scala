package com.lolo.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
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
        ssc.sparkContext.setLogLevel("error")

        // 保存数据的状态，需要设定检查点的路径
        ssc.sparkContext.setCheckpointDir("checkpoint")

        //创建连接kafka的参数
        val brokeList = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
        val consumerGroup = "test"
        val kafkaParams: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokeList,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest", //earliest、latest
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean) //默认为true
        )
        val topics = Array("testTopic")
        // 从kafka中采集数据
        val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )
        val kafkaDStream: DStream[(String, String)] = stream.map(x => (x.key(), x.value()))


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

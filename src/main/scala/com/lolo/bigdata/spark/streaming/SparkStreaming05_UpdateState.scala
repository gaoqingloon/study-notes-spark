package com.lolo.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: 状态更新(有一个buffer)，需要设置checkpoint
  * version: 1.0
  */
object SparkStreaming05_UpdateState {

    /**
      * 该程序存在的问题，当程序结束后再启动，不会记住之前消费数据的位置
      * 通常把offset累加到redis中
      */
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("updateState")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

        // 保存数据的状态，需要设定检查点的路径
        ssc.sparkContext.setCheckpointDir("checkpoint")
        ssc.sparkContext.setLogLevel("error")

        // 从kafka中采集数据
        /*val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
            ssc,
            "hadoop102:2181,hadoop103:2181,hadoop104:2181",
            "gordon",
            Map("gordon" -> 3)
        )*/

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



        // 将采集的数据进行分解（扁平化）
        val wordDStream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))

        // 将数据进行结构的转换方便统计分析
        val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

        // 将转换结构后的数据进行聚合处理（有状态）
        //val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
        val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
            case (seq, buffer) => {
                val sum = buffer.getOrElse(0) + seq.sum
                Option(sum)
            }
        }

        /*val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey(
            updateFunc,
            new HashPartitioner(ssc.sparkContext.defaultParallelism),
            rememberPartitioner = true
        )*/


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
      * 创建匿名函数 val func: x => y = x1 => y1
      * 第一个参数：聚合的key，就是单词
      * 第二个参数：当前批次产生批次该单词在每一个分区出现的次数
      * 第三个参数：初始值或累加的中间结果
      */
    val updateFunc: Iterator[(String, Seq[Int], Option[Int])] => Iterator[(String, Int)] = {
        data => {
            // data.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
            data.map {
                case (x, y, z) => (x, y.sum + z.getOrElse(0))
            }
        }
    }

    // 传参需要使用匿名函数，只是一段逻辑，不能使用这种函数
    def updateFunc1(data: Iterator[(String, Seq[Int], Option[Int])]): Iterator[(String, Int)] = {
        data.map {
            // data.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
            case (x, y, z) => (x, y.sum + z.getOrElse(0))
        }
    }
}

package com.lolo.bigdata.spark.streaming

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/20/2019
  * Description: 向kafka中的topic生成用户随机的访问日志
  * version: 1.0
  */
object ProduceDataToKafka {

    def main(args: Array[String]): Unit = {

        val topic = "testTopic"

        //kafka参数信息
        val props = new Properties()
        val brokeList = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokeList)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

        //kafka生产者
        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

        //向topic中生成随机数据
        var counter = 0
        var keyFlag = 0
        while (true) {
            counter += 1
            keyFlag += 1
            val content: String = userLogs()
            producer.send(new ProducerRecord[String, String](topic, s"key-$keyFlag", content))

            if (0 == counter % 200) {
                counter = 0
                Thread.sleep(2000)
            }
        }

        //关闭kafka生产者
        producer.close()
    }

    /**
      * 生成用户随机的访问日志
      *
      * @return
      */
    def userLogs(): String = {
        val userLogBuffer = new StringBuffer()
        val timestamp = new Date().getTime
        var userId = 0L
        var pageId = 0L

        //随机生成的用户ID
        userId = Random.nextInt(2000)

        //随机生成的页面ID
        pageId = Random.nextInt(2000)

        //随机生成Channel
        val channelNames = Array[String]("spark", "scala", "kafka", "flink", "hadoop", "storm", "hive", "impala", "hbase", "ml")
        val channel = channelNames(Random.nextInt(channelNames.length))

        //随机生成行为
        val actionNames = Array[String]("view", "register")
        val action = actionNames(Random.nextInt(actionNames.length))

        val dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

        userLogBuffer.append(dateToday)
            .append("\t")
            .append(timestamp)
            .append("\t")
            .append(userId)
            .append("\t")
            .append(pageId)
            .append("\t")
            .append(channel)
            .append("\t")
            .append(action)

        System.out.println(userLogBuffer.toString)
        userLogBuffer.toString
    }

}

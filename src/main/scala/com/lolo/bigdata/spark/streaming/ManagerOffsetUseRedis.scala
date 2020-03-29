package com.lolo.bigdata.spark.streaming

import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/20/2019
  * Description: 利用redis 来维护消费者偏移量
  * version: 1.0
  */
object ManagerOffsetUseRedis {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setMaster("local[*]")
        conf.setAppName("ManagerOffsetUseRedis")
        //设置每个分区每秒读取多少条数据
        conf.set("spark.streaming.kafka.maxRatePerPartition", "10")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        //设置日志级别
        ssc.sparkContext.setLogLevel("ERROR")


        val topic = "testTopic"

        /**
          * 从redis中获取消费者offset
          */
        val dbIndex = 3 //select 3
        val currentTopicOffset: mutable.Map[String, String] = getOffSetFromRedis(dbIndex, topic)
        //初始读取到的topic offset
        currentTopicOffset.foreach(x => println(s"初始读取到的offset: $x"))

        //转换成需要的类型
        val fromOffsets: Predef.Map[TopicPartition, Long] = currentTopicOffset.map {
            resultSet => new TopicPartition(topic, resultSet._1.toInt) -> resultSet._2.toLong
        }.toMap


        //创建连接kafka的参数
        val brokeList = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
        val consumerGroup = "MyGroupId"

        val kafkaParams: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokeList,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest" //latest、earliest
        )

        /**
          * 将获取到的消费者offset传递给SparkStreaming
          */
        val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
        )

        stream.foreachRDD { rdd =>

            println("*********** 业务处理完成 ***********")

            val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            rdd.foreachPartition { _ =>
                val o: OffsetRange = offsetRanges(TaskContext.get().partitionId())
                println(s"topic:${o.topic}, partition: ${o.partition}, " +
                    s"fromOffset: ${o.fromOffset}, untilOffset: ${o.untilOffset}")
            }

            //将当前批次最后的所有分区offsets保存到redis中
            saveOffsetToRedis(dbIndex, offsetRanges)
        }

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }

    /**
      * 从redis中获取保存的消费者offset
      *
      * @param db
      * @param topic
      * @return
      */
    def getOffSetFromRedis(db: Int, topic: String): mutable.Map[String, String] = {
        val jedis = RedisClient.pool.getResource
        jedis.select(db)
        val result: util.Map[String, String] = jedis.hgetAll(topic)
        //释放jedis连接资源
        // RedisClient.pool.returnResource(jedis)
        // starting from Jedis 3.0 this method won't exist.
        // Resouce cleanup should be done using redis.clients.jedis.Jedis.close()
        if (jedis != null) {
            jedis.close()
        }

        if (result.size() == 0) {
            result.put("0", "0")
            result.put("1", "0")
            result.put("2", "0")
        }
        import scala.collection.JavaConversions.mapAsScalaMap
        val offsetMap: mutable.Map[String, String] = result
        offsetMap
    }

    /**
      * 将消费者offset保存到redis中
      *
      * @param db
      * @param offsetRanges
      */
    def saveOffsetToRedis(db: Int, offsetRanges: Array[OffsetRange]): Unit = {
        val jedis: Jedis = RedisClient.pool.getResource
        jedis.select(db)
        offsetRanges.foreach {
            one => jedis.hset(one.topic, one.partition.toString, one.untilOffset.toString)
        }
    }
}

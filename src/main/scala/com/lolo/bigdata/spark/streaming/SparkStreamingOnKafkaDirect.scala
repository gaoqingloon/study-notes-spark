package com.lolo.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
 * Author: gordon  Email:gordon_ml@163.com
 * Date: 11/20/2019
 * Description:
 * version: 1.0
 */

/**
  * SparkStreaming2.x版本读取kafka中数据：
  *
  * 1. 采用了新的消费者API实现，类似于1.6中SparkStreaming读取kafka Direct模式。并行度一样。
  *
  * 2. 因为采用了新的消费者api实现，所有相对于1.6的Direct模式【simple api实现】，api使用上有很大差别。未来这种api有可能继续变化。
  *
  * 3. kafka中有两个参数：
  *     heartbeat.interval.ms:
  *         这个值代表kafka集群与消费者之间的心跳间隔时间，kafka集群确保消费者保持连接的心跳通信时间间隔。这个时间默认是3s。
  *         这个值必须设置的比session.timeout.ms 小，一般设置不大于 session.timeout.ms 的1/3。
  *     session.timeout.ms :
  *         这个值代表消费者与kafka之间的session会话超时时间，如果在这个时间内，kafka没有接收到消费者的心跳【heartbeat.interval.ms控制】，
  *         那么kafka将移除当前的消费者。这个时间默认是10s。
  *         这个时间是位于设置 group.min.session.timeout.ms【6s】和group.max.session.timeout.ms【300s】之间的一个参数，如果SparkStreaming批次间隔大于5分钟，
  *         也就是大于300s，那么就要相应的调大group.max.session.timeout.ms这个值。
  *
  * 4. 大多情况下，SparkStreaming读取数据使用 LocationStrategies.PreferConsistent 这种策略，这种策略会将分区均匀的分布在集群的Executor之间。
  *     如果Executor在kafka 集群中的某些节点上，可以使用 LocationStrategies.PreferBrokers 这种策略，那么当前这个Executor中的数据会来自当前broker节点。
  *     如果节点之间的分区有明显的分布不均，可以使用 LocationStrategies.PreferFixed 这种策略，可以通过一个map指定将topic分区分布在哪些节点中。
  *
  * 5. 新的消费者api可以将kafka中的消息预读取到缓存区中，默认大小为64k。默认缓存在Executor中，加快处理数据速度。
  *     可以通过参数 spark.streaming.kafka.consumer.cache.maxCapacity来增大，也可以通过spark.streaming.kafka.consumer.cache.enabled设置成false关闭缓存机制。
  *
  * 6. 关于消费者offset
  *     1).如果设置了checkpoint，那么offset将会存储在checkpoint中。
  *     这种有缺点: 第一，当从checkpoint中恢复数据时，有可能造成重复的消费，需要我们写代码来保证数据的输出幂等。
  *               第二，当代码逻辑改变时，无法从checkpoint中来恢复offset。
  *     2).依靠kafka来存储消费者offset，kafka中有一个特殊的topic来存储消费者offset。新的消费者api中，会定期自动提交offset。这种情况有可能也不是我们想要的，
  *         因为有可能消费者自动提交了offset，但是后期SparkStreaming没有将接收来的数据及时处理保存。这里也就是为什么会在配置中将enable.auto.commit设置成false的原因。
  *         这种消费模式也称最多消费一次，默认SparkStreaming拉取到数据之后就可以更新offset，无论是否消费成功。自动提交offset的频率由参数auto.commit.interval.ms决定，默认5s。
  *         如果我们能保证完全处理完业务之后，可以后期异步的手动提交消费者offset。
  *
  *     3).自己存储offset，这样在处理逻辑时，保证数据处理的事务，如果处理数据失败，就不保存offset，处理数据成功则保存offset，这样可以做到精确的处理一次处理数据。
  *
  */
object SparkStreamingOnKafkaDirect {

    /**
      * 新Api没有Receiver模式
      */
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setMaster("local[*]")
        conf.setAppName("SparkStreamingOnKafkaDirect")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        //设置日志级别
        ssc.sparkContext.setLogLevel("ERROR")

        //创建连接kafka的参数
        val brokeList = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
        val consumerGroup = "MyGroupId"

        val kafkaParams: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokeList,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,

            /**
              * 当没有初始的offset，或者当前的offset不存在，如何处理数据
              * earliest : 自动重置偏移量为最小偏移量
              * latest : 自动重置偏移量为最大偏移量【默认】
              * none : 没有找到以前的offset，抛出异常
              */
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",

            /**
              * 当设置 enable.auto.commit 为false时，不会自动向kafka中保存消费者offset，需要异步的处理完数据之后手动提交
              */
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean) //默认为true
        )

        val topics = Array("testTopic")
        val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )

        val transStream: DStream[String] = stream.map(record => {
            val key_value = (record.key(), record.value())
            println(s"receive message key = ${key_value._1}")
            println(s"receive message value = ${key_value._2}")
            key_value._2
        })

        val wordsDS: DStream[String] = transStream.flatMap(line => line.split(" "))
        val result: DStream[(String, Int)] = wordsDS.map((_, 1)).reduceByKey(_ + _)
        result.print()

        /**
          * 以上业务处理完成之后，异步的提交消费者offset，这里将 enable.auto.commit 设置为false，就是使用kafka 自己来管理消费者offset
          * 注意这里，获取 offsetRanges: Array[OffsetRange] 每一批次topic中的offset时，必须从源头读取过来的stream中获取，不能从经过的stream中获取
          */
        stream.foreachRDD { rdd =>
            val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            // some time later, after outputs have completed
            stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}

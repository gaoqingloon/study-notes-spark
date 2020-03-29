package com.lolo.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: customReceiver
  * version: 1.0
  */
object SparkStreaming03_CustomReceiver {

    def main(args: Array[String]): Unit = {

        // spark配置对象
        val conf = new SparkConf().setMaster("local[*]").setAppName("customReceiver")

        // 实时数据分析环境对象，采集周期：以指定的时间为周期采集实时数据
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

        // 1. 从指定的端口中采集数据
        /*val socketLineDStream: ReceiverInputDStream[String] =
            ssc.socketTextStream("hadoop102", 9999)*/
        // 1. 从指定文件夹中采集数据
        //val fileDStream: DStream[String] = ssc.textFileStream("spark-warehouse")
        // 1. 自定义Receiver
        val receiverDStream: ReceiverInputDStream[String] = ssc.receiverStream(
            new MyReceiver("hadoop102", 9999))


        // 将采集的数据进行分解（扁平化）
        val wordDStream: DStream[String] = receiverDStream.flatMap(line => line.split(" "))

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

/**
  * 自定义采集器（接收器）
  * 1) 继承Receiver。 extends Receiver[String](StorageLevel.MEMORY_ONLY)
  * 2) 重写方法 onStart，onStop
  */
class MyReceiver(host: String, port: Int)
    extends Receiver[String](StorageLevel.MEMORY_ONLY) { // String就是接收数据的类型

    // 采集器的启动
    override def onStart(): Unit = {
        // Start the thread that receives data over a connection
        /*new Thread("Socket Receiver") {
            override def run() {
                receive()
            }
        }.start()*/
        new Thread(new Runnable {
            override def run(): Unit = {
                receive()
            }
        }, "Socket Receiver").start()
    }

    // 采集器的停止
    override def onStop(): Unit = {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
        if (socket != null) {
            socket.close()
            socket = null
        }
    }


    var socket: Socket = _

    /** Create a socket connection and receive data until receiver is stopped */
    def receive(): Unit = {

        var userInput: String = null

        try {
            // Connect to host:port
            socket = new Socket(host, port)

            // Until stopped or connection broken continue reading
            // 生成输入流
            val reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

            userInput = reader.readLine()
            while (!isStopped && userInput != null) {
                // 将采集的数据存储到采集器的内部进行转换
                if ("END".equals(userInput)) {
                    return
                }
                // 內部的函数，将数据存储下去
                this.store(userInput)
                userInput = reader.readLine()
            }
            reader.close()
            socket.close()

            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again")

        } catch {
            case e: java.net.ConnectException =>
                // restart if could not connect to server
                restart("Error connecting to " + host + ":" + port, e)
            case t: Throwable =>
                // restart if there is any other error
                restart("Error receiving data", t)
        }
    }
}

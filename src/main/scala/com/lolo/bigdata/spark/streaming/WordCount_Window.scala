package com.lolo.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by gql on 2019-7-29 20:34:09.
  *
  * 若两个窗口交叉的少，使用原始的
  * 若两个窗口交叉的多，使用+ -
  */
object WordCount_Window {

    def main(args: Array[String]) {

        val conf = new SparkConf().setMaster("local[2]").setAppName("window")
        // batchInterval = 3s
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./checkpoint")

        // Create a DStream that will connect to hostname:port, like localhost:9999
        val lines = ssc.socketTextStream("hadoop102", 9000)

        // Split each line into words
        val words = lines.flatMap(_.split(" "))

        //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
        // Count each word in each batch
        val pairs = words.map(word => (word, 1))

        //val wordCounts = pairs.reduceByKey((a:Int,b:Int) => (a + b))

        // 若两个窗口交叉的少，使用原始的
        // 若两个窗口交叉的多，使用+ -
        // 窗口大小 12s， 12/3 = 4
        // 滑动步长 6S，   6/3 = 2
        //val wordCounts = pairs.reduceByKeyAndWindow((a:Int, b:Int) => (a + b), Seconds(12), Seconds(6))

        //val wordCounts2 = pairs.reduceByKeyAndWindow(_ + _, _ - _, Seconds(12), Seconds(6))
        val wordCounts2 = pairs.reduceByKeyAndWindow(
            (x: Int, y: Int) => x + y,
            (x: Int, y: Int) => x - y,
            Seconds(12),
            Seconds(6)
        )

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts2.print()

        ssc.start() // Start the computation
        ssc.awaitTermination() // Wait for the computation to terminate
        //ssc.stop()
    }
}

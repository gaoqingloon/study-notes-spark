package com.lolo.bigdata.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: 广播变量 broadcast
  * version: 1.0
  */
object Spark52_Broadcast {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("broadcast")
        val sc = new SparkContext(conf)

        val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))

        // 1. 使用普通方式进行join，存在shuffle
        val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
        val joinRDD: RDD[(Int, (String, Int))] = rdd1.join(rdd2)
        joinRDD.foreach(println)
        /*
        (1,(a,1))
        (3,(c,3))
        (2,(b,2))
         */

        // 2. 使用广播变量减少数据的传输
        val list = List((1, 1), (2, 2), (3, 3))
        // 1) 构建广播变量（发送到每个Executor）
        val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

        val resultRDD: RDD[(Int, (String, Any))] = rdd1.map {
            case (key, value) => {
                var v2: Any = null
                // 2) 使用广播变量
                for (elem <- broadcast.value) {
                    if (key == elem._1) {
                        v2 = elem._2
                    }
                }
                (key, (value, v2))
            }
        }
        resultRDD.foreach(println)

        sc.stop()
    }
}

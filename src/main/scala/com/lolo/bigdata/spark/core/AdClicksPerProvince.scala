package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: 统计出每一个省份广告被点击次数的TOP3
  * version: 1.0
  */
object AdClicksPerProvince {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("adClick").setMaster("local[*]")
        val sc = new SparkContext(conf)

        /*
        时间戳，省份，城市，用户，广告
        1516609143867 6 7 64 16
        1516609143869 9 4 75 18
        1516609143869 1 7 87 12
         */
        val fileRDD: RDD[String] = sc.textFile("data/agent.log")

        // 1. 河北,XXX  => 河北_XXX, 1
        val proAndAdToOne: RDD[(String, Int)] = fileRDD.map(line => {
            val fields: Array[String] = line.split(" ")
            val province = fields(1)
            val ad = fields(4)
            (province + "_" + ad, 1)
        })
        //proAndAdToOne.collect().take(3).foreach(println)

        // 2. 河北_XXX, 1  => 河北_XXX, 21
        val proAndAdToSum: RDD[(String, Int)] = proAndAdToOne.reduceByKey(_ + _)
        //proAndAdToSum.collect().take(3).foreach(println)

        // 3. 河北_XXX, 21 => 河北,(XXX,21)
        // map groupByKey
        val proToAdAndSum: RDD[(String, (String, Int))] = proAndAdToSum.map {
            case (key, value) =>
                val proAndAd = key.split("[_]")
                val pro = proAndAd(0)
                val ad = proAndAd(1)
                (pro, (ad, value))
        }
        val groupPro: RDD[(String, Iterable[(String, Int)])] = proToAdAndSum.groupByKey()
        //groupPro.collect().take(3).foreach(println)

        // 4. 河北,(XXX,21) 降序排序
        val proAndTop3: RDD[(String, List[(String, Int)])] = groupPro.mapValues(x => {
            x.toList.sortWith((x, y) => x._2 > y._2).take(3)
        })
        proAndTop3.collect().foreach(println)

        /*
        (4,List((12,25), (2,22), (16,22)))
        (8,List((2,27), (20,23), (11,22)))
        (6,List((16,23), (24,21), (22,20)))
        (0,List((2,29), (24,25), (26,24)))
        (2,List((6,24), (21,23), (29,20)))
        (7,List((16,26), (26,25), (1,23)))
        (5,List((14,26), (21,21), (12,21)))
        (9,List((1,31), (28,21), (0,20)))
        (3,List((14,28), (28,27), (22,25)))
        (1,List((3,25), (6,23), (5,22)))
         */

        sc.stop()
    }
}

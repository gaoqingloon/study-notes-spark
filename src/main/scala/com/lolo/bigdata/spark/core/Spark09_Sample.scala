package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/15/2019
  * Description: sample 算子
  * version: 1.0
  */
object Spark09_Sample {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：以指定的随机种子随机抽样出数量为fraction的数据，
        withReplacement表示是抽出的数据是否放回，true为有放回
        的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。

        2. 需求：创建一个RDD（1-10），从中选择放回和不放回抽样
         */
        val conf = new SparkConf()
            .setAppName("sample").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

        // false不放回，按0.4概率抽取，随机种子1
        val sampleRDD: RDD[Int] = listRDD.sample(withReplacement = false, 0.4, 1)
        //val sampleRDD: RDD[Int] = listRDD.sample(withReplacement = true, 4, 1)
        sampleRDD.collect().foreach(println)

        /*
        3
        5
        6
        8
         */
        sc.stop()
    }
}

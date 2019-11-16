package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: pipe算子
  * version: 1.0
  */
object Spark14_Pipe {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：管道，针对每个分区，都执行一个shell脚本，返回输出的RDD。
        注意：脚本需要放在Worker节点可以访问到的位置

        2. 需求：编写一个脚本，使用管道将脚本作用于RDD上。
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("pipe")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[String] = sc.makeRDD(List("hi","Hello","how","are","you"), 1)
        // command: String
        val pipeRDD: RDD[String] = listRDD.pipe("D:\\sw\\Git\\bin\\bash.exe data/pipe.sh")
        pipeRDD.glom().collect().foreach(x => print(x.mkString(", ")))
        println()
        //AA, >>>hi, >>>Hello, >>>how, >>>are, >>>you

        // 分成两个分区，每个分区各自执行一次sh脚本
        val listRDD2: RDD[String] = sc.makeRDD(List("hi","Hello","how","are","you"), 2)
        val pipeRDD2: RDD[String] = listRDD2.pipe("D:\\sw\\Git\\bin\\bash.exe data/pipe.sh")
        pipeRDD2.glom().collect().foreach(x => print(x.mkString(", ")))
        //pipeRDD2.collect().foreach(x => print(x + ", "))
        println()
        // AA, >>>hi, >>>HelloAA, >>>how, >>>are, >>>you  AA出现两次

        sc.stop()
    }
}

package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/16/2019
  * Description: partitionBy算子
  * version: 1.0
  */
object Spark20_PartitionPy {

    def main(args: Array[String]): Unit = {

        /*
        1. 作用：对pairRDD进行分区操作，如果原有的partionRDD和现有的
            partionRDD是一致的话就不进行分区， 否则会生成ShuffleRDD，
            即会产生shuffle过程。

        2. 需求：创建一个4个分区的RDD，对其重新分区
         */
        val conf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[(Int, String)] =
            sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)

        println(listRDD.partitions.length) //4

        // &: 分区数必须是2^n，均匀分布（2^n-1）
        // %: 分区数任意
        // partitioner: Partitioner
        val partitionByRDD: RDD[(Int, String)] =
        listRDD.partitionBy(new HashPartitioner(2))

        val resTupleArray: Array[Array[(Int, String)]] =
            partitionByRDD.glom().collect()

        resTupleArray.foreach(x => print(x + ", "))
        println() //[Lscala.Tuple2;@3e6f3bae, [Lscala.Tuple2;@12477988,
        resTupleArray.foreach(x => print(x.mkString(", ")))
        println() //(2,bbb), (4,ddd)(1,aaa), (3,ccc)//2个分区


        /** 使用自定义分区器 */
        val myPartitionByRDD: RDD[(Int, String)] =
            listRDD.partitionBy(new MyPartitioner(3))

        val myResTupleArray: Array[Array[(Int, String)]] =
            myPartitionByRDD.glom().collect()

        println(myPartitionByRDD.partitions.length) //3
        myResTupleArray.foreach(x => print(x + ", "))
        println() //[Lscala.Tuple2;@2234078, [Lscala.Tuple2;@5ec77191, [Lscala.Tuple2;@4642b71d,
        myResTupleArray.foreach(x => print(x.mkString(", ")))
        println() //(1,aaa), (2,bbb), (3,ccc), (4,ddd)//3个分区但只有一个分区有数据

        // 也可以这样测试，3个文件，只有一个文件有数据
        myPartitionByRDD.saveAsTextFile("out")

        sc.stop()
    }
}

// 声明自定义分区器
class MyPartitioner(partitions: Int) extends Partitioner {

    // 指定分区个数
    override def numPartitions = {
        partitions
    }

    // 根据分区个数书写分区规则
    override def getPartition(key: Any) = {
        1
    }
}

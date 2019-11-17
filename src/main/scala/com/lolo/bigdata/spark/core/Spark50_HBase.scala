package com.lolo.bigdata.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: 操作 HBase
  * version: 1.0
  */
object Spark50_HBase {

    def main(args: Array[String]): Unit = {

        // Spark配置
        val config = new SparkConf().setMaster("local[*]").setAppName("hbase")
        val sc = new SparkContext(config)

        // HBase表配置
        val conf: Configuration = HBaseConfiguration.create()
        conf.set(TableInputFormat.INPUT_TABLE, "rdd")

        // 查询HBase数据
        //searchData(sc, conf)

        // 插入数据到HBase
        inputData(sc, conf)

        sc.stop()
    }

    /**
      * 从HBase中查询数据
      * newAPIHadoopRDD
      */
    def searchData(sc: SparkContext, conf: Configuration): Unit = {

        // 返回结构: 主键+查询结果
        val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
            conf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )
        hBaseRDD.foreach {
            case (_, result) => {
                val cells: Array[Cell] = result.rawCells()
                for (cell <- cells) {
                    println(Bytes.toString(CellUtil.cloneValue(cell)))
                }
            }
        }
        //gordon
    }

    /**
      * 插入数据到HBase
      * saveAsHadoopDataset()
      */
    def inputData(sc: SparkContext, conf: Configuration): Unit = {

        val dataRDD: RDD[(String, String)] =
            sc.makeRDD(List(("1002", "lnn"), ("1003", "tony"), ("1004", "tom")))

        val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
            case (rowKey, name) => {
                val put: Put = new Put(Bytes.toBytes(rowKey))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
                (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
            }
        }

        // 指定数据插入到哪一张表
        val jobConf = new JobConf(conf)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "rdd")

        putRDD.saveAsHadoopDataset(jobConf)
    }
}

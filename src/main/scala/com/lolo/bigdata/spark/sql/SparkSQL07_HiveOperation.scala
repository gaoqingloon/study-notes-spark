package com.lolo.bigdata.spark.sql

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description: Hive
  * version: 1.0
  */
object SparkSQL07_HiveOperation {

    def main(args: Array[String]) {

        val warehouseLocation = new File("spark-warehouse").getAbsolutePath

        val spark = SparkSession
            .builder()
            .appName("Spark Hive Example")
            .config("spark.sql.warehouse.dir", warehouseLocation)
            .enableHiveSupport()  // 开启对hive的支持
            .getOrCreate()

        //import spark.implicits._

        spark.sql("CREATE TABLE IF NOT EXISTS spark_hive (key INT, value STRING)")
        spark.sql("LOAD DATA LOCAL INPATH 'data/kv1.txt' INTO TABLE spark_hive")

        // Queries are expressed in HiveQL
        val df: DataFrame = spark.sql("SELECT * FROM spark_hive")
        df.show()

        df.write.format("json").save("hdfs://hadoop102:9000/spark-hive-test.json")

        spark.stop()
    }
}

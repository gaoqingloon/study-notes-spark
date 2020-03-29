package com.lolo.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/18/2019
  * Description:
  * version: 1.0
  */
object Practice {

    /**
      * 将DataFrame插入到Hive表
      */
    private def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame): Unit = {
        spark.sql("DROP TABLE IF EXISTS " + tableName)
        dataDF.write.saveAsTable(tableName)
    }

    private def insertMySQL(tableName: String, dataDF: DataFrame): Unit = {
        dataDF.write
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/sparksql")
            .option("dbtable", tableName)
            .option("user", "root")
            .option("password", "root")
            .mode(SaveMode.Overwrite)
            .save()
    }

    def main(args: Array[String]): Unit = {
        // 创建Spark配置
        val sparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]")

        // 创建Spark SQL 客户端
        val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        import spark.implicits._

        // 加载数据到Hive
        val tbStockRdd = spark.sparkContext.textFile("data/tbStock.txt")
        val tbStockDS = tbStockRdd.map(_.split(",")).map(attr => tbStock(attr(0), attr(1), attr(2))).toDS
        insertHive(spark, "tbStock", tbStockDS.toDF)

        val tbStockDetailRdd = spark.sparkContext.textFile("data/tbStockDetail.txt")
        val tbStockDetailDS = tbStockDetailRdd.map(_.split(",")).map(attr => tbStockDetail(attr(0), attr(1).trim().toInt, attr(2), attr(3).trim().toInt, attr(4).trim().toDouble, attr(5).trim().toDouble)).toDS
        insertHive(spark, "tbStockDetail", tbStockDetailDS.toDF)

        val tbDateRdd = spark.sparkContext.textFile("data/tbDate.txt")
        val tbDateDS = tbDateRdd.map(_.split(",")).map(attr => tbDate(attr(0), attr(1).trim().toInt, attr(2).trim().toInt, attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)).toDS
        insertHive(spark, "tbDate", tbDateDS.toDF)

        //需求一： 统计所有订单中每年的销售单数、销售总额
        val result1 = spark.sql("SELECT c.theYear, COUNT(DISTINCT a.orderNumber), SUM(b.amount) FROM tbStock a JOIN tbStockDetail b ON a.orderNumber = b.orderNumber JOIN tbDate c ON a.dateId = c.dateId GROUP BY c.theYear ORDER BY c.theYear")
        insertMySQL("xq1", result1)

        //需求二： 统计每年最大金额订单的销售额
        val result2 = spark.sql("SELECT theYear, MAX(c.sumOfAmount) AS sumOfAmount FROM (SELECT a.dateId, a.orderNumber, SUM(b.amount) AS sumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.orderNumber = b.orderNumber GROUP BY a.dateId, a.orderNumber ) c JOIN tbDate d ON c.dateId = d.dateId GROUP BY theYear ORDER BY theYear DESC")
        insertMySQL("xq2", result2)

        //需求三： 统计每年最畅销货品
        val result3 = spark.sql("SELECT DISTINCT e.theYear, e.itemId, f.maxOfAmount FROM (SELECT c.theYear, b.itemId, SUM(b.amount) AS sumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.orderNumber = b.orderNumber JOIN tbDate c ON a.dateId = c.dateId GROUP BY c.theYear, b.itemId ) e JOIN (SELECT d.theYear, MAX(d.sumOfAmount) AS maxOfAmount FROM (SELECT c.theYear, b.itemId, SUM(b.amount) AS sumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.orderNumber = b.orderNumber JOIN tbDate c ON a.dateId = c.dateId GROUP BY c.theYear, b.itemId ) d GROUP BY d.theYear ) f ON e.theYear = f.theYear AND e.sumOfAmount = f.maxOfAmount ORDER BY e.theYear")
        insertMySQL("xq3", result3)

        spark.close
    }
}

case class tbStock(orderNumber: String, locationId: String, dateId: String) extends Serializable

case class tbStockDetail(orderNumber: String, rowNum: Int, itemId: String, number: Int, price: Double, amount: Double) extends Serializable

case class tbDate(dateId: String, years: Int, theYear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfMonth: Int) extends Serializable

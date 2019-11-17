package com.lolo.bigdata.spark.core

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/17/2019
  * Description: JdbcRDD 操作 MySQL
  * version: 1.0
  */
object Spark50_MySQL {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("jdbcRDD")
        val sc = new SparkContext(conf)

        // MySQL 连接参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql:///rdd"
        val userName = "root"
        val password = "123456"

        //searchData(sc, driver, url, userName, password)
        //insertData(sc, driver, url, userName, password)
        insertDataAdvance(sc, driver, url, userName, password)

        sc.stop()
    }

    def searchData(sc: SparkContext, driver: String, url: String,
                   userName: String, password: String): Unit = {
        val sql = "select * from user where id >= ? and id <= ?"
        val jdbcRDD: JdbcRDD[Unit] = new JdbcRDD(
            sc,
            () => {
                // 创建JdbcRDD连接数据库
                Class.forName(driver)
                java.sql.DriverManager.getConnection(url, userName, password)
            },
            sql,
            1, //上限
            3, //下限
            2,
            (rs) => {
                println(rs.getString(1) + ", " + rs.getString(2))
            }
        )
        /*
        1, zhangsan
        2, lisi
        3, wangwu
         */

        jdbcRDD.collect()
    }

    /**
      * 对每条数据建立一次连接（效率低）
      */
    def insertData(sc: SparkContext, driver: String, url: String,
                   userName: String, password: String): Unit = {

        val dataRDD: RDD[(String, Int)] =
            sc.makeRDD(List(("gordon", 25), ("lnn", 18), ("tony", 21)))

        dataRDD.foreach {
            case (username, age) =>
                Class.forName(driver)
                // 连接对象无法加序列化
                val connection = java.sql.DriverManager.getConnection(url, userName, password)

                val sql = "insert into user(name, age) values(?, ?)"
                val statement = connection.prepareStatement(sql)
                statement.setString(1, username)
                statement.setInt(2, age)
                statement.executeUpdate()

                statement.close()
                connection.close()
        }
    }

    /**
      * 优化：对每个分区建立一次连接
      * 注意：OOM的问题
      */
    def insertDataAdvance(sc: SparkContext, driver: String, url: String,
                          userName: String, password: String): Unit = {

        val dataRDD: RDD[(String, Int)] =
            sc.makeRDD(List(("gordon", 25), ("lnn", 18), ("tony", 21)))

        dataRDD.foreachPartition(data => {
            Class.forName(driver)
            // 连接对象无法加序列化
            val connection = java.sql.DriverManager.getConnection(url, userName, password)

            data.foreach({
                case (username, age) => {

                    val sql = "insert into user(name, age) values(?, ?)"
                    val statement = connection.prepareStatement(sql)
                    statement.setString(1, username)
                    statement.setInt(2, age)
                    statement.executeUpdate()

                    statement.close()
                }
            })

            connection.close()
        })
    }
}

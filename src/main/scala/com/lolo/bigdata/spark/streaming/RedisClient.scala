package com.lolo.bigdata.spark.streaming

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/20/2019
  * Description:
  * version: 1.0
  */
object RedisClient {

    val redisHost = "hadoop102"
    val redisPost = 6379
    val redisTimeout = 30000 //ms

    /**
      * JedisPool是一个连接池，既可以保证线程安全，又可以保证较高的效率
      */
    lazy val pool = new JedisPool(new GenericObjectPoolConfig, redisHost, redisPost, redisTimeout)

    /**
      * 测试连接
      * @param args
      */
    def main(args: Array[String]): Unit = {
        val jedis = RedisClient.pool.getResource
        println(jedis)
        //redis.clients.jedis.Jedis@1b40d5f0
    }
}

package com.lolo.bigdata.spark.core.examples

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/21/2019
  * Description: 生成模拟PV，UV数据
  * version: 1.0
  */
object ProducePvAndUvData {

    //IP
    val IP = 223
    //地址
    val ADDRESS = Array("北京市", "天津市", "上海市", "重庆市", "河北省",
        "山西省", "辽宁省", "吉林省", "黑龙江省", "江苏省", "浙江省", "安徽省", "福建省",
        "江西省", "山东省", "河南省", "湖北省", "湖南省", "广东省", "海南省", "四川省",
        "贵州省", "云南省", "陕西省", "甘肃省", "青海省", "台湾省", "内蒙古自治区",
        "广西壮族自治区", "西藏自治区", "宁夏回族自治区", "新疆维吾尔自治区", "香港特别行政区", "澳门特别行政区")

    //日期
    val DATE: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    //timestamp
    val TIMESTAMP = 0L
    //userId
    val USERID = 0L
    //网站
    val WEBSITE = Array("www.baidu.com", "www.taobao.com", "www.dangdang.com", "www.jd.com",
        "www.suning.com", "www.mi.com", "www.google.com", "www.hirain.com")

    //行为
    val ACTION = Array("register", "comment", "view", "login", "buy", "click", "logout")

    def main(args: Array[String]): Unit = {

        val pathFileName = "data/pvuvdata.log"

        //创建文件
        val createFile = CreateFile(pathFileName)

        //向文件中写入数据,需要的对象 new PrintWriter
        val file = new File(pathFileName)
        val fos = new FileOutputStream(file, true)
        val osw = new OutputStreamWriter(fos, "UTF-8")
        val pw = new PrintWriter(osw)

        if (createFile) {

            //产生5万+数据
            val dataCount = 50000
            for (_ <- Range(0, dataCount, 1)) { //for (_ <- 0 until 5) {
                val random = new Random()

                //模拟一个ip
                val ip = random.nextInt(IP) + "." + random.nextInt(IP) + "." + random.nextInt(IP) + "." + random.nextInt(IP)
                //模拟地址
                val address = ADDRESS(random.nextInt(ADDRESS.length))
                //模拟日期
                val date = DATE
                //模拟userId
                val userId = Math.abs(random.nextLong())

                /**
                  * 这里的while模拟是同一个用户不同时间点对不同网站的操作
                  */
                var j = 0
                var timestamp = 0L
                var webSite = "未知网站"
                var action = "未知行为"
                val flag = random.nextInt(5) | 1

                while (j < flag) {
                    //模拟timestamp
                    timestamp = new Date().getTime
                    //模拟网站
                    webSite = WEBSITE(random.nextInt(WEBSITE.length))
                    //模拟行为
                    action = ACTION(random.nextInt(ACTION.length))
                    j += 1
                }

                /**
                  * 拼接
                  */
                val content = ip + "\t" + address + "\t" + date + "\t" + timestamp +
                    "\t" + userId + "\t" + webSite + "\t" + action
                println(content)
                //向文件中写入数据
                pw.write(content + "\n")
            }
        }

        //关闭的先后顺序，先打开的后关闭，后打开的先关闭
        pw.close()
        osw.close()
        fos.close()
    }

    /**
      * 创建文件
      *
      * @param pathFileName
      * @return
      */
    def CreateFile(pathFileName: String): Boolean = {
        val file = new File(pathFileName)
        if (file.exists())
            file.delete()
        val createNewFile = file.createNewFile()
        println("create file " + pathFileName + " success!")
        createNewFile
    }

}


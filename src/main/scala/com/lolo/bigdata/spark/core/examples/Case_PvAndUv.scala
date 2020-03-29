package com.lolo.bigdata.spark.core.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Author: gordon  Email:gordon_ml@163.com
  * Date: 11/21/2019
  * Description: 案例练习
  * Version: 1.0
  */
object Case_PvAndUv {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("pvAndUv").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("error")

        // 203.184.153.76	海南省	2019-11-21	1574314069708	320429471967037838	www.suning.com	click
        // 180.71.125.139	四川省	2019-11-21	1574314069708	2657275481326155896	www.jd.com	register

        // pv: (sitesite, count)
        // uv: (ip/uid-sitesite, count)
        // 每个site每个地区访问量取前3

        // 1.pv
        //PVStatics(sc)


        // 2.uv
        //180.71.125.139,www.jd.com
        //180.71.125.139,www.jd.com
        //180.71.125.131,www.jd.com

        //180.71.125.139,www.jd.com
        //180.71.125.131,www.jd.com

        //www.jd.com,2

        //UVStatics(sc)


        //3. 每个网址每个地区访问量取前3
        //海南省,www.taobao.com
        //海南省,www.taobao.com
        //海南省,www.jd.com

        //海南省,www.jd.com,199
        //海南省,www.taobao.com,89
        val lines = sc.textFile("data/pvuvdata.log")

        //test1(sc, lines)
        test2(sc, lines)
    }


    /**
      * 统计每个网址每个地区的访问量
      */
    def test2(sc: SparkContext, lines: RDD[String]): Unit = {

        val siteAndLocalToOne: RDD[((String, String), Int)] = lines.map { line =>
            val fields: Array[String] = line.split("\t")
            ((fields(5), fields(1)), 1)
        }

        val siteAndLocalToSum: RDD[((String, String), Int)] = siteAndLocalToOne.reduceByKey(_ + _)

        val siteToLocalAndSum: RDD[(String, (String, Int))] = siteAndLocalToSum.map { 
            case (siteAndLocal, count) => (siteAndLocal._1, (siteAndLocal._2, count))
        }

        val siteToGroup: RDD[(String, Iterable[(String, Int)])] = siteToLocalAndSum.groupByKey()

        val result: RDD[(String, List[(String, Int)])] = siteToGroup.mapValues { t =>
            // f: Iterable[(String, Int)] => U 需要 Iterable.toList   list.iterator
            t.toList.sortWith((x, y) => x._2 > y._2).take(3)
            //t.toList.sortBy(one => -one._2).take(3)
        }

        result.collect().foreach(println)

        /*
        (www.suning.com,List((河南省,204), (天津市,203), (辽宁省,203)))
        (www.jd.com,List((广西壮族自治区,224), (山东省,211), (陕西省,208)))
        (www.dangdang.com,List((青海省,211), (陕西省,207), (湖北省,207)))
        (www.taobao.com,List((江西省,215), (广东省,203), (四川省,202)))
        (www.mi.com,List((陕西省,210), (新疆维吾尔自治区,206), (澳门特别行政区,206)))
        (www.hirain.com,List((江西省,230), (北京市,209), (浙江省,204)))
        (www.baidu.com,List((湖南省,212), (陕西省,206), (台湾省,203)))
        (www.google.com,List((四川省,211), (江西省,211), (广东省,201)))
         */
    }

    /**
      * 统计每个地区的每个网址访问量
      */
    def test1(sc: SparkContext, lines: RDD[String]): Unit = {

        val localAndSiteToOne: RDD[((String, String), Int)] = lines.map { line =>
            val fields: Array[String] = line.split("\t")
            ((fields(1), fields(5)), 1)
        }

        val localAndSiteToSum: RDD[((String, String), Int)] = localAndSiteToOne.reduceByKey(_ + _)

        val localToSiteAndSum: RDD[(String, (String, Int))] = localAndSiteToSum.map {
            case (localAndSite, count) => (localAndSite._1, (localAndSite._2, count))
        }

        val localToGroup: RDD[(String, Iterable[(String, Int)])] = localToSiteAndSum.groupByKey()

        val result: RDD[(String, List[(String, Int)])] = localToGroup.mapValues { t =>
            // f: Iterable[(String, Int)] => U 需要 Iterable.toList   list.iterator
            t.toList.sortWith((x, y) => x._2 > y._2).take(3)
            //t.toList.sortBy(one => -one._2).take(3)
        }

        result.collect().foreach(println)
        /*
        (湖北省,List((www.dangdang.com,207), (www.taobao.com,201), (www.jd.com,188)))
        (宁夏回族自治区,List((www.suning.com,202), (www.taobao.com,196), (www.dangdang.com,183)))
        (天津市,List((www.suning.com,203), (www.mi.com,202), (www.baidu.com,193)))
        (甘肃省,List((www.google.com,184), (www.baidu.com,183), (www.hirain.com,183)))
        ......
         */
    }

    /**
      * pv
      * @param sc
      */
    def PVStatics(sc: SparkContext, lines: RDD[String]): Unit = {

        val siteAndOne: RDD[(String, Int)] = lines.map { line =>
            val fields: Array[String] = line.split("\t")
            (fields(5), 1)
        }

        val siteAndSum: RDD[(String, Int)] = siteAndOne.reduceByKey(_ + _)

        val result: RDD[(String, Int)] = siteAndSum.sortBy(_._2, ascending = false)

        val top3: Array[(String, Int)] = result.take(8)

        top3.foreach(println)
    }
    /*
    (www.suning.com,6393)
    (www.hirain.com,6286)
    (www.baidu.com,6262)
    (www.taobao.com,6257)
    (www.mi.com,6238)
    (www.jd.com,6200)
    (www.dangdang.com,6195)
    (www.google.com,6169)
     */
    

    /**
      * uv
      * @param sc
      */
    def UVStatics(sc: SparkContext, lines: RDD[String]): Unit = {

        val ipAndSite: RDD[(String, String)] = lines.map { line =>
            val fields: Array[String] = line.split("\t")
            (fields(0), fields(5))
        }

        // uv的去重
        val distinctIpAndSite: RDD[(String, String)] = ipAndSite.distinct()

        val siteAndOne: RDD[(String, Int)] = distinctIpAndSite.map(t => (t._2, 1))

        val result: RDD[(String, Int)] = siteAndOne.reduceByKey(_ + _).sortBy(_._2, ascending = false)

        result.take(8).foreach(println)
    }
    /*
    (www.suning.com,6393)
    (www.hirain.com,6286)
    (www.baidu.com,6262)
    (www.taobao.com,6256)
    (www.mi.com,6238)
    (www.jd.com,6200)
    (www.dangdang.com,6195)
    (www.google.com,6169)
     */
}

package com.lolo.bigdata.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: gordon  Email:gordon_ml@163.com
 * @Date: 11/21/2019
 * @Description:
 * @Version: 1.0
 */
public class SparkCore01_JavaRDD {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("local[*]");
        conf.setMaster("test");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile("data/word.txt");

        List<String> stringList = lines.collect();
        for (String s : stringList) {
            System.out.println(s);
        }

        String first = lines.first();
        System.out.println(first);

        List<String> take = lines.take(3);
        System.out.println(take);


        JavaRDD<String> sample = lines.sample(true, 0.1, 1);
        sample.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        JavaRDD<String> filterRDD = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                return "hello spark".equals(line);
            }
        });

        long count = filterRDD.count();
        System.out.println(count);

        filterRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        JavaRDD<String> mapRDD = lines.map(new Function<String, String>() { //<String, String>输入String，输出String
            @Override
            public String call(String line) throws Exception {
                return line + "*";
            }
        });


        JavaPairRDD<String, Integer> mapToPairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaRDD<String> flatMapRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                List<String> words = Arrays.asList(line.split(" "));
                return words.iterator();
            }
        });

        JavaRDD<String> flatMapRDD1 = lines.flatMap(
                (FlatMapFunction<String, String>) line -> {
                    List<String> words = Arrays.asList(line.split(" "));
                    return words.iterator();
                }
        );

        JavaPairRDD<String, Integer> reduceByKeyRDD = mapToPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String, Integer>> result = reduceByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> wordAndCount) throws Exception {
                return new Tuple2<>(wordAndCount._2, wordAndCount._1);
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> countAndWord) throws Exception {
                return new Tuple2<>(countAndWord._2, countAndWord._1);
            }
        }).collect();

        for (Tuple2<String, Integer> res : result) {
            System.out.println(res);
        }


        List<Tuple2<String, Integer>> collect = reduceByKeyRDD.collect();
        for (Tuple2<String, Integer> v : collect) {
            System.out.println(v);
        }


        jsc.stop();
    }
}

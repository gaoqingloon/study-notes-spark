package com.lolo.bigdata.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
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
public class SparkCore02_JavaWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("WordCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 1. testFile
        JavaRDD<String> lines = jsc.textFile("data/word.txt");

        // 2. flatMap
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                List<String> words = Arrays.asList(line.split(" "));
                return words.iterator();
            }
        });

        // 3. map
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        // 4. reduceByKey
        JavaPairRDD<String, Integer> wordAndCount = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 5. sortBy
        JavaPairRDD<Integer, String> countAndWord = wordAndCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> wordOne) throws Exception {
                return new Tuple2<>(wordOne._2, wordOne._1);
            }
        });
        JavaPairRDD<Integer, String> sorted = countAndWord.sortByKey(false);
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> countWord) throws Exception {
                return new Tuple2<>(countWord._2, countWord._1);
            }
        });

        // 6. foreach
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> result) throws Exception {
                System.out.println(result.toString());
            }
        });

        // collect
        /*List<Tuple2<String, Integer>> res = result.collect();
        for (Tuple2<String, Integer> w : res) {
            System.out.println(w);
        }*/


        jsc.stop();
    }
}

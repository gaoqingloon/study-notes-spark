package com.lolo.bigdata.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Author: gordon  Email:gordon_ml@163.com
 * @Date: 11/21/2019
 * @Description:
 * @Version: 1.0
 */
public class SparkCore03_JavaRDD {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("test").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> nameRDD =
                sc.parallelizePairs(Arrays.asList(new Tuple2<>("a", 10),
                        new Tuple2<>("b", 10), new Tuple2<>("c", 10)));
        JavaPairRDD<String, Integer> scoreRDD =
                sc.parallelizePairs(Arrays.asList(new Tuple2<>("a", 100),
                        new Tuple2<>("b", 100),
                        new Tuple2<>("c", 100)));

        JavaPairRDD<String, Integer> union = nameRDD.union(scoreRDD);
        union.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

    }
}

package org.example;
//Spark este compatibil cu Java 11
//Pentru a rula programul fara probleme trebuie sa adaugam in VM options:
//--add-exports java.base/sun.nio.ch=ALL-UNNAMED


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;


public class Main {
    public static void main(String[] args) {
//        SparkSession spark = SparkSession.builder().appName("Read Parquet File").master("local").getOrCreate();
//
//        Dataset<Row> dataFrame = spark.read().parquet("/Users/segio/Desktop/License/src/main/resources/parquet/andorra_28-09-2022.osm.pbf.node.parquet");
//
//        dataFrame.show();

        SparkConf conf = new SparkConf().setAppName("Read Parquet File").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3));
        JavaRDD<Integer> filtered = rdd.filter(x -> x == 2);

        System.out.println(filtered.first());

    }
}
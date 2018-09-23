package com.demo.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

//利用sparksql操作hivesql
object HiveSupport {
  def main(args: Array[String]): Unit = {
      //1、创建sparkSession
      val spark: SparkSession = SparkSession.builder()
                                .appName("HiveSupport")
                                .master("local[2]")
                                .config("spark.sql.warehouse.dir","d:\\spark-warehouse")
                                .enableHiveSupport() //开启对hive的支持
                                .getOrCreate()
      //2、获取sparkContext
      val sc: SparkContext = spark.sparkContext
      sc.setLogLevel("WARN")

     //3、通过sparkSession操作hivesql
      //创建一个hive表
      spark.sql("create table if not exists student(id int,name string,age int)" + "row format delimited fields terminated by ','")
      //加载数据到hive表里面
      spark.sql("load data local inpath './upload/person.txt' into table student")
     //查询表的结果数据
      spark.sql("select * from student").show()


    //关闭
    sc.stop()
    spark.stop()
  }

}

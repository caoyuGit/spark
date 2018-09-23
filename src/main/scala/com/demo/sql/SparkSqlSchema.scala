package com.demo.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Spark SQL查询:通过反射推断Schema
  */

object SparkSqlSchema {


  def main(args: Array[String]): Unit = {
    //1 创建SparkContext对象
    val spark: SparkSession = SparkSession.builder().appName("CaseClassSchema").master("local[2]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    //设置日志输出级别
    sc.setLogLevel("WARN")

    //2 读取数据
    val file: RDD[String] = sc.textFile("A:\\File\\person.txt")
    val lines: RDD[Array[String]] = file.map(_.split(" "))

    //3 RDD转换成DataFrame
    //加载数据到Row对象中
    val personRDD: RDD[Row] = lines.map(x => Row(x(0).toInt, x(1), x(2).toInt))
    //创建schema
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("age", IntegerType, false)
    ))
    //利用personRDD与schema创建DataFrame
    val personDF: DataFrame = spark.createDataFrame(personRDD, schema)

    //4 DataFram操作数据
    //将DataFrame注册成表
    personDF.createTempView("t_person")

    spark.sql("select * from t_person where age>30").show
    spark.sql("select * from t_person order by age desc").show()

    //5 关闭资源
    sc.stop()
    spark.stop()
  }
}

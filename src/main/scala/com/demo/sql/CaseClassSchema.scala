package com.demo.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Spark SQL查询:通过反射推断Schema
  */

//Person样例类
case class Person(id: Int, name: String, age: Int)

object CaseClassSchema {
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
    //将RDD与Person类关联
    val personRDD: RDD[Person] = lines.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF()

    //4 DataFram操作数据
    //4.1 DSL语法操作
    //4.1.1 显示DataFrame的数据
    //    personDF.show()
    //4.1.2 显示DataFrame的schema
    //    personDF.printSchema()
    //4.1.3 显示DataFrame记录数
    //    println(personDF.count())
    //4.1.4 显示DataFrame的所有字段
    //    personDF.columns.foreach(println)
    //4.1.5 取出DataFrame的第一行记录
    //    personDF.head()
    //4.1.6 显示DataFrame中name字段的所有值
    //    println(personDF.col("name"))
    //4.1.7 过滤出DataFrame中年龄大于30的记录
    //    personDF.filter($"age" > 30).show()
    //4.1.8 统计DataFrame中年龄大于30的人数
    //    println(personDF.filter($"age" > 30).count())
    //4.1.9 统计DataFrame中按照年龄进行分组，求每个组的人数
    //    personDF.groupBy("age").count().show()

    //4.2 SQL操作风格
    //将DataFrame注册成表
    personDF.createTempView("t_person")

    spark.sql("select * from t_person where age>30").show
    spark.sql("select * from t_person order by age desc").show()

    //5 关闭资源
    sc.stop()
    spark.stop()
  }
}

package com.demo.sql

import java.io.FileReader
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

//todo:利用sparksql从mysql中加载数据
object DataFromMysql {
  def main(args: Array[String]): Unit = {
    //1、创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("DataFromMysql")
      .master("local[2]")
      .getOrCreate()

    //2、通过sparkSession获取mysql表中的数据
    // 读取resources下的配置文件
    val properties = new Properties()
    properties.load(this.getClass().getResourceAsStream("/database.properties"))
    //设置用户名和密码
    //properties.setProperty("user","root")
    //properties.setProperty("password","123")

    val dataFrame: DataFrame = spark.read.jdbc("jdbc:mysql:///spark", "iplocation", properties)

    //打印dataFrame schema信息
    dataFrame.printSchema()

    //打印dataFrame中的数据
    dataFrame.show()

    //关闭
    spark.stop()
  }

}

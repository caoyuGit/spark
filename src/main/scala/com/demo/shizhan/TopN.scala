package com.demo.shizhan

/**
  * 计算pv
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopN {

  def main(args: Array[String]): Unit = {
    //1  获取上下文对象
    val config = new SparkConf().setAppName("topN").setMaster("local[2]")
    val sparkContext = new SparkContext(config)

    //2 读取数据
    val file: RDD[String] = sparkContext.textFile("A:\\File\\access.log")

    //3 transformation阶段
    //读取一行数据
     val pageAndOne: RDD[Array[String]] = file.map(_.split(" ")).filter(_.length>10).filter(!_(10).equals("\"-\""))

    //记录数据
    val map: RDD[(String, Int)] = pageAndOne.map(x=>(x(10),1))

    //4 action阶段
    //聚合并排序
     val pages: RDD[(String, Int)] = map.reduceByKey(_+_).sortBy(_._2,false)
    //获取前n个
    val topN: Array[(String, Int)] = pages.take(args(0).toInt)
    topN.foreach(println)

    //5 关闭资源
    sparkContext.stop()
  }

}

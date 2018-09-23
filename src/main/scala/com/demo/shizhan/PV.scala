package com.demo.shizhan

/**
  * 计算pv
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PV {

  def main(args: Array[String]): Unit = {
    //1  获取上下文对象
    val config = new SparkConf().setAppName("pv").setMaster("local[2]")
    val sparkContext = new SparkContext(config)

    //2 读取数据
    val file: RDD[String] = sparkContext.textFile("A:\\File\\access.log")

    //3 transformation阶段
    //每行数据记为一条pv
    val pvAndOne: RDD[(String, Int)] = file.map(_x=> ("pv", 1))

    //4 action阶段
    val pvs: RDD[(String, Int)] = pvAndOne.reduceByKey(_ + _)
    pvs.foreach(println)

    //5 关闭资源
    sparkContext.stop()
  }

}

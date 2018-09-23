package com.demo.shizhan

/**
  * 计算UV
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UV {

  def main(args: Array[String]): Unit = {
    //1  获取上下文对象
    val config = new SparkConf().setAppName("uv").setMaster("local[2]")
    val sparkContext = new SparkContext(config)

    //2 读取数据
    val file: RDD[String] = sparkContext.textFile("A:\\File\\access.log")

    //3 transformation阶段
    //对每一行分隔，获取IP地址
    val ips: RDD[String] = file.map(_.split(" ")).map(x => x(0))
    //对ip地址进行去重
    val pvAndOne: RDD[(String, Int)] = ips.distinct().map(_ => ("uv", 1))

    //4 action阶段
    //聚合输出
    val pvs: RDD[(String, Int)] = pvAndOne.reduceByKey(_ + _)
    pvs.foreach(println)

    //5 关闭资源
    sparkContext.stop()
  }

}

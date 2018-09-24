package com.demo.wordCount

/**
  * wordCount案例单机模式
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1 设置程序入口
    //设置 spark 的配置文件信息,设置appName和Master地址
    val sparkConf = new SparkConf().setAppName("WordConut").setMaster("local[2]")
    //构建 sparkcontext 上下文对象，它是程序的入口,所有计算的源头
    val sc = new SparkContext(sparkConf);

    //2 读取文件
    val file: RDD[String] = sc.textFile("A:/files/words.txt")

    //3 处理文件
    //对文件中每一行单词进行压平切分
    val flatMap = file.flatMap(_.split(" "))
    //对每一个单词计数为 1 转化为(单词，1)
    val map = flatMap.map((_,1))
    //相同的单词进行汇总 前一个下划线表示累加数据，后一个下划线表示新数据
    val result: RDD[(String, Int)] = map.reduceByKey(_+_)

    //按单词出现的频率降序排列
    val sortResult: RDD[(String, Int)] = result.sortBy(_._2,false)

    //4 结果处理
    //收集结果数据
    val collect: Array[(String, Int)] = sortResult.collect()
    //输出数据
    collect.foreach(x=>println(x))

    //保存数据到 HDFS
    //reslut.saveAsTextFile("/wc")

    //5 关闭资源
    sc.stop()
  }
}

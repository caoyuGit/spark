package com.demo.stream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

//todo:利用sparkStreaming接受socket数据，实现所有批次单词计数结果累加
object SparkStreamingSocketTotal {

  //定义一个方法
  //currentValues:他表示在当前批次每个单词出现的所有的1   (hadoop,1) (hadoop,1)(hadoop,1)
  //historyValues:他表示在之前所有批次中每个单词出现的总次数   (hadoop,100)
  def updateFunc(currentValues:Seq[Int], historyValues:Option[Int]):Option[Int] = {
          val newValue: Int = currentValues.sum+historyValues.getOrElse(0)
          Some(newValue)
  }

  def main(args: Array[String]): Unit = {
      //1、创建sparkConf
      val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketTotal").setMaster("local[2]")
      //2、创建sparkContext
      val sc = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
     //3、创建streamingContext
      val ssc = new StreamingContext(sc,Seconds(5))
     //设置checkpoint目录
      ssc.checkpoint("./ck")
    //4、接受socket数据
      val stream: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    //5、切分每一行
      val words: DStream[String] = stream.flatMap(_.split(" "))
    //6、把每一个单词计为1
      val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //7、相同单词出现的次数累加
      val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)

    //8、打印结果数据
      result.print()

    //9、开启流式计算
      ssc.start()
      ssc.awaitTermination()
    }
}

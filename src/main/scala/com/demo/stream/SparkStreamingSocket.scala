package com.demo.stream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

//todo:利用sparkStreaming接受socket数据，实现单词计数
object SparkStreamingSocket {
  def main(args: Array[String]): Unit = {
     //1、创建sparkConf   设置master的地址local[N] ,n必须大于1，其中1个线程负责去接受数据，另一线程负责处理接受到的数据
      val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocket").setMaster("local[2]")
    // 2、创建sparkContext
      val sc = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
    //3、创建streamingContext,需要sparkContext和以多久时间间隔为一个批次
     val ssc = new StreamingContext(sc,Seconds(5))
    //4、通过streaming接受socket数据
      val stream: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    //5、切分每一行
      val words: DStream[String] = stream.flatMap(_.split(" "))
    //6、每个单词记为1
      val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //7、相同单词出现的次数累加
      val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //8、打印
      result.print()

    //9、开启流式计算
      ssc.start()
      //一直会阻塞，等待退出
      ssc.awaitTermination()
  }
}

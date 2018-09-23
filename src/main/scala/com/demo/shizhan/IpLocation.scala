package com.demo.shizhan

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ip地址查询
  */
object IpLocation {
  def main(args: Array[String]): Unit = {
    //1 设置SparkContext
    val config: SparkConf = new SparkConf().setAppName("ipLocation").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(config)

    //2 读取信息
    // 城市ip信息
    val ipLocation: RDD[String] = sc.textFile("A:\\File\\ip.txt")
    //ip日志
    val ipLog: RDD[String] = sc.textFile("A:\\File\\20090121000132.394251.http.format")

    //3 根据ipLocation,获取ciry和ip对应的元祖集合
    val cityAndIp: RDD[(String, String, String, String)] = ipLocation.map(_.split("\\|")).map(x => (x(2), x(3), x(13), x(14)))

    //转换成广播
    val cityAndIpBroadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(cityAndIp.collect())

    //4 从ip日志中获取ip
    val ips: RDD[String] = ipLog.map(_.split("\\|")(1))

    //5 将ip日志中的每条ip与iplocation对应
    //5.1 获取ip日志中的每条ip
    val result: RDD[((String, String), Int)] = ips.mapPartitions(iterator => {
      // 获取ipLocation广播中的值
      val cityAndIpValue: Array[(String, String, String, String)] = cityAndIpBroadcast.value

      //从ip迭代器中获取ip
      iterator.map(ip => {
        //将ip转换成数值
        val ipLong: Long = ipChange.ip2Long(ip)
        //匹配ip与城市ip信息
        val index: Int = ipChange.ip2Location(ipLong, cityAndIpValue)
        //转化成localtion
        ((cityAndIpValue(index)._3, cityAndIpValue(index)._4), 1)
      })
    })

    //6、把相同经度和维度出现的次数累加
    val finalResult: RDD[((String, String), Int)] = result.reduceByKey(_ + _)

    //7、打印输出结果
    finalResult.collect().foreach(x => println(x))

    //8保存结果数据到mysql表中
    //finalResult.foreachPartition(SaveData.data2mysql(_))

    //9、关闭sc
    sc.stop()
  }

}

object ipChange {
  //匹配ip与城市ip信息,使用插值查找,返回角标
  def ip2Location(ip2Long: Long, cityAndIpValue: Array[(String, String, String, String)]): Int = {
    var first = 0
    var last = cityAndIpValue.length - 1

    while (first <= last) {
      //val tmp: Long = (ip2Long-cityAndIpValue(first)._1.toLong)/(cityAndIpValue(last)._2.toLong-cityAndIpValue(first)._1.toLong)
      //var middle=(first+(last-first)*tmp).toInt
      var middle: Int = (first + last) / 2
      if (ip2Long < cityAndIpValue(middle)._1.toLong) {
        last = middle
      } else if (ip2Long > cityAndIpValue(middle)._2.toLong) {
        first = middle
      } else {
        return middle
      }
    }

    //找不到
    return -1
  }

  // 将ip转换成数值
  def ip2Long(ip: String): Long = {
    var ipNum: Long = 0L
    val ips: Array[String] = ip.split("\\.")
    for (i <- ips) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }
}

object SaveData {
  //将数据存入数据库
  def data2mysql(tuples: Iterator[((String, String), Int)]): Unit = {
    var conn: Connection = null
    var prepareStatement: PreparedStatement = null
    //编写sql语句
    val sql = "insert into iplocation(longitude,latitude,total_count) values(?,?,?)"

    //获取连接
    conn=DriverManager.getConnection("jdbc:mysql://localhost/spark","root","123")
    prepareStatement = conn.prepareStatement(sql)

    tuples.foreach(tuple => {
      //给sql语句中的占位符赋值
      prepareStatement.setString(1, tuple._1._1)
      prepareStatement.setString(2, tuple._1._2)
      prepareStatement.setInt(3, tuple._2)

      // 执行插入语句
      prepareStatement.execute()
    })
  }

}

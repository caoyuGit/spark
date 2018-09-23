package com.demo.sql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo:利用sparksql将数据写入到mysql表中

case class Student(id:Int,name:String,age:Int)
object DataToMysql {
  def main(args: Array[String]): Unit = {
      //1、创建sparkSession
      val spark: SparkSession = SparkSession.builder().appName("DataToMysql").master("local[2]")getOrCreate()
      //2、通过sparkSession获取sparkContext
       val sc: SparkContext = spark.sparkContext
      sc.setLogLevel("WARN")
     //3、通过sparkContext加载数据文件
      val data: RDD[String] = sc.textFile("A:/File/person.txt")
     //4、切分每一行
      val linesArrayRDD: RDD[Array[String]] = data.map(_.split(" "))
     //5、linesArrayRDD与样例类关联
      val personRDD: RDD[Student] = linesArrayRDD.map(x=>Student(x(0).toInt,x(1),x(2).toInt))
     //6、将personRDD转化为dataframe

      //手动导入隐式转换
      import spark.implicits._
      val personDF: DataFrame = personRDD.toDF

      //personDF注册成一张表
    personDF.createTempView("t_student")
    //打印dataframe中的结果数据
    personDF.show()

    //操作表
    val resultDF: DataFrame = spark.sql("select * from t_student order by age desc")

    //将resultDF写入到mysql表中
      val properties = new Properties()
     properties.setProperty("user","root")
     properties.setProperty("password","123456")

    //mode需要对应4个参数
    //overwrite:覆盖（它会帮你创建一个表，然后进行覆盖）
    //append:追加（它会帮你创建一个表，然后把数据追加到表里面）
    //ignore:忽略（它表示只要当前表存在，它就不会进行任何操作）
    //ErrorIfExists:只要表存在就报错（默认选项）
    resultDF.write.mode("overwrite").jdbc("jdbc:mysql://node1:3306/test","stu",properties)

    //关闭
    sc.stop()
    spark.stop()

  }
}

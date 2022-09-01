package org.example


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description TODO
 * @author hadoop
 * @date 2022/8/15 16:04
 */
object 编程实现RDD转换DataFrame {
  //样例类
  case class Student(id:String, name:String,score:Double)

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("RDD2DataFrame").set("dfs.client.use.datanode.hostname", "true")
    val sc=new SparkContext(conf)



    //创建SparkSession
    val spark=SparkSession.builder().config(conf).getOrCreate()
    //引入import spark.implicits._
    import spark.implicits._
    val hdfsHost="hdfs://master:9000"
    //从文件中创建RDD
    val fileRDD=sc.textFile(hdfsHost+"/user/spark/data/score.txt")
    val studentRDD=fileRDD.map(line=>{
      val datas=line.split("\\s+")
      Student(datas(0),datas(1),datas(2).toDouble)
    })
    val df:DataFrame =studentRDD.toDF()
    //查询成绩>=80的学生
    df.filter($"score">=80).show()
    //创建临时表
    df.createOrReplaceTempView("student")
    spark.sql("select * from student where score >=80").show()
  }

}

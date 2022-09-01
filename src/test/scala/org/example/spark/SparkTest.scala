package org.example.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
 * @description TODO
 * @author hadoop
 * @date 2022/8/10 10:14
 */
class SparkTest extends Serializable{

  @Test
  def test1={
    //创建sparkConf对象--spark启动对象
    val conf=new SparkConf().setMaster("local[*]").setAppName("test1")
    //启动应用程序入口
    val sc=new SparkContext(conf)
    val rdd1:RDD[Int]=sc.makeRDD(List(10,20,30))
    val rdd2:RDD[(String,Int)]=sc.makeRDD(List(("a",12),("b",22)))
    rdd1.foreach(println)
    rdd2.foreach(println)
  }
  @Test
  def test2={
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext=new SparkContext(sparkConf)
    //检查output文件是否存在,存在就删除
    val conf = sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val step1_path = new org.apache.hadoop.fs.Path("/user/spark/output")
    val exists = fs.exists(step1_path)
    if (exists) fs.delete(step1_path, true)
    val hdfsHost="hdfs://master:9000"

    //从文件中创建RDD
    val fileRDD=sparkContext.textFile(hdfsHost+"/user/spark/input")
    //按一个或者多个空格分离单词，并生成RDD
    val lines=fileRDD.flatMap(_.split("\\s+"))
    val result = lines.map((_,1)).reduceByKey(_+_)
    result.foreach(println)
    result.saveAsTextFile(hdfsHost+"/user/spark/output")
  sparkContext.stop()
  }
  @Test
  //1、filter
  //将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
  def test3(): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("filter")
    val sparkContext=new SparkContext(sparkConf)
    val dataRDD=sparkContext.makeRDD(List(1,2,3,4))
    val result=dataRDD.filter(_%2==0)
    result.foreach(println)
    sparkContext.stop()
  }

  @Test
  def ScoreTest(): Unit ={
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("ScoreTest")
    val sparkContext=new SparkContext(sparkConf)
    //读取本地文件，file:/// + 绝对路径
    val rdd1=sparkContext.textFile("e:/temp/score.txt")
    val rdd2=rdd1.map(line=>{
      val lines=line.split("\t")
      Student(lines(0),lines(1),lines(2).toInt)
    })
    val rdd3=rdd2.sortBy(_.score,ascending = false).take(5)
    rdd3.foreach(println)

  }
  //创建样例类，实现序列化，extends Serializable
  case class Student(id:String, name:String,score:Int)

  @Test
  def Top3={
    val conf=new SparkConf().setMaster("local[*]").setAppName("ScoreTest")
    val sc=new SparkContext(conf)
    //读取数据文件：时间戳、省份、城市、用户、广告以空格隔开
    val dataRDD=sc.textFile("e:/temp/bar.txt")
    //转换数据类型 数据格式：（省份-广告，1）
    val tuplesRDD =dataRDD.map(line =>{
      val lines=line.split("\\s+")
      (lines(1)+"-"+lines(4),1)
    })
    //对数据进行聚合
    val reducedRDD =tuplesRDD.reduceByKey(_+_)
    //格式转换：（省份，（广告，sum值））
    val iterableRDD=reducedRDD.map(line=>{
      (line._1.split("-")(0),(line._1.split("-")(1),line._2))
    })
    //对省份再进行聚合，数据格式：（省份，（（广告，sum值），（广告，sum值）...））
    val groupRDD=iterableRDD.groupByKey()
    //选择Top3,reverse对值进行降序排列，然后取前三
    val resultRDD=groupRDD.mapValues(_.toList.sortBy(_._2).reverse.take(3))
    resultRDD.foreach(println)
    sc.stop()

  }
  @Test
  def Emp={
    val conf=new SparkConf().setMaster("local[*]").setAppName("Emp")
    val sc=new SparkContext(conf)
    val rdd1=sc.textFile("e:/temp/emp.txt")
    //统计各个部门薪水最高的员工的姓名和薪水（工资+奖金）=》（部门，（姓名，薪水））
    rdd1.map(line=>{
        val lines=line.split("\t")
      if(lines(6).isEmpty){
       lines(6)="0"
      }
        Emp1(lines(0),lines(1),lines(2),lines(3),lines(4),lines(5).toInt,lines(6).toInt,lines(7))

      //groupByKey之后变成了迭代器，不能直接操作，所以要toList
    }).map(emp=>{
      (emp.deptno,(emp.name,emp.salary+emp.comm))
    }).groupByKey().flatMapValues(_.toList.sortBy(_._2).reverse.take(1)).foreach(println)
    println
    case class Emp1(empno:String,name:String,job:String,leader:String,date:String,salary:Int,comm:Int,deptno:String)

    //统计各个职务任职人数=》（职务，人数）
    rdd1.map(line=>{
      val lines=line.split("\t")
      (lines(2),1)
    }).reduceByKey(_+_).foreach(println)
    println

    //统计每个领导的员工姓名及工资=》（领导，（姓名，工资），...）
    rdd1.map(line=>{
      val lines=line.split("\t")
      if(lines(6)==""){
        (lines(3),(lines(1),lines(5).toInt))
      }else{
        (lines(3),(lines(1),lines(5).toInt+lines(6).toInt))
      }
    }).groupByKey().flatMapValues(_.toList).foreach(println)
    sc.stop()
  }

  @Test
  def Broadast={
    val conf=new SparkConf().setMaster("local[*]").setAppName("Emp")
    val sc=new SparkContext(conf)
    val dataRDD=sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)),4)
    val list=List(("a",10),("b",20),("c",30),("d",40))

    //方法一:join
    val listRDD=sc.makeRDD(list)
    val rdd1=dataRDD.join(listRDD)
    rdd1.foreach(println)
//
    //方法二:cogroup
    val cogroupRDD=dataRDD.cogroup(listRDD)
    cogroupRDD.foreach(println)
    val rdd2=cogroupRDD.mapValues({
      case(x,y)=>{
        (x.sum,y.sum)
      }
    })
    rdd2.foreach(println)


    /**
     * 将dataRDD与list匹配=>(("a",(1,10)),("b",(2,20)),("c",(3,30)),("d",(4,40)))
     * dataRDD分为4个分区,即每个job有4个task,list就要分发到每个task
     * 如果把list创建为广播变量,每个task共享这个广播变量
     */
    //创建广播变量
    val listBroadcast:Broadcast[List[(String,Int)]]=sc.broadcast(list)
    val resultRDD=dataRDD.map({
      case(key,value) =>{
        var num=0
        for ((k,v)<-listBroadcast.value){
          if(key.equals(k)){
            num=v
          }
        }
        (key,(value,num))
      }
    })
    resultRDD.foreach(println)
    sc.stop()
  }
  @Test
  //累加器
  def Accumulator={
    val conf=new SparkConf().setMaster("local[*]").setAppName("Emp")
    val sc=new SparkContext(conf)
    val rdd = sc.makeRDD(List(1,2,3,4,5))
    //声明累加器
    val lator=sc.longAccumulator("lator")
    var sum=0
    rdd.foreach(num=>{
      //使用累加器
      lator.add(num)
      sum+=num
    })
    //获取累加器的值
    println("lator="+lator.value)
    println("sum="+sum)
    sc.stop()

  }



}

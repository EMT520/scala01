package org.example

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description TODO
 * @author hadoop
 * @date 2022/8/12 17:07
 */
object Key_Value类型 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("key_value")
    val sc=new SparkContext(conf)
    val rdd1=sc.makeRDD(List(("a",5),("b",3),("c",1),("a",2)))
    //1.reduceByKey:可以将数据按照相同的 Key 对 Value 进行聚合
    val rdd2=rdd1.reduceByKey(_+_)
    rdd2.foreach(println)
    println

    //2、groupByKey:将数据源的数据根据 key 对 value 进行分组
    val rdd3=rdd1.groupByKey()
    rdd3.foreach(println)

    //3、aggregateByKey
    //将数据根据不同的规则进行分区内计算和分区间计算。
    //● 第一个参数表示分区内的计算规则
    //● 第二个参数表示分区间的计算规则
    //当调用（K，V）对的数据集时，返回（K，U）对的数据集，其中使用给定的组合函数和 zeroValue 聚合每个键的值。
    val rdd4=sc.makeRDD(List(("a",3),("a",2),("c",4),("c",3),("d",6),("d",8)),2)

    /**
     * 1.分区0：("a",3),("a",2),("c",4)  分区1:("c",3),("d",6),("d",8)
     * 2.zeroValue聚合=>分区0：a->(0,3,2),c->(0,4) 分区1：c->(0,3),d->(0,6,8)
     * 3.max分区内计算=>分区0：a->(0,3),c->(0,4)  分区1:c->(0,3),d->(0,8)
     * 4. +分区间计算("a",3)("c",7)("d",8)
     */
    val rdd5=rdd4.aggregateByKey(zeroValue=0)(Math.max(_,_),_+_)
    rdd5.foreach(println)

    //4、join
    //在一个 (K, V) 和 (K, W) 类型的 Dataset 上调用时，返回一个 (K, (V, W)) 的 Dataset，等价于内连接操作。
    // 如果想要执行外连接，可以使用 leftOuterJoin, rightOuterJoin 和 fullOuterJoin 等算子
    val rdd6=sc.makeRDD(List((1,"a"),(2,"b"),(3,"c"),(4,"d")))
    val rdd7=sc.makeRDD(List((1,10),(2,20),(3,30)))
    val joinRDD=rdd6.join(rdd7)
    println("==joinRDD==")
    joinRDD.foreach(println)
    val leftRDD=rdd6.leftOuterJoin(rdd7)
    println("==leftRDD==")
    leftRDD.foreach(println)
    val rightRDD=rdd6.rightOuterJoin(rdd7)
    println("==rightRDD==")
    rightRDD.foreach(println)

    //5、cogroup
    //在一个 (K, V) 对的 Dataset 上调用时，返回多个类型为 (K, (Iterable, Iterable)) 的元组所组成的 Dataset
    val rdd8=sc.makeRDD(List(("a",1),("a",2),("c",3)))
    val rdd9=sc.makeRDD(List(("a",1),("c",2),("c",3)))
    val rdd10=rdd8.cogroup(rdd9)
    rdd10.foreach(println)
    println

    //6.mapValues
    //原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素
    val rdd11=sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val rdd12=rdd11.mapValues(_*10)
    rdd12.foreach(println)
    println

    //7、flatMapValues
    //每个一元素的Value被输入函数映射为一系列的值，然后这些值再与原RDD中的Key组成一系列新的KV对。
    val rdd13=sc.makeRDD(List(("a",2),("b",4)))
    val rdd14=rdd13.flatMapValues(x=>List(x,10))
    println("==flatMapValues==")
    rdd14.foreach(println)
    val rdd15=rdd13.mapValues(x=>List(x,10))
    println("==mapValues==")
    rdd15.foreach(println)


    sc.stop()

  }

}

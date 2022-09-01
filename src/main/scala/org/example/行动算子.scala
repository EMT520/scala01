package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description Action类算子也是一类算子叫做行动算子，如foreach,collect，count等。Transformations类算子是延迟执行，Action类算子是触发执行。一个application应用程序中有几个Action类算子执行，就有几个job运行。
 *              常见Action类算子
 *              ● count：返回数据集中的元素数。会在结果计算完成后回收到Driver端。
 *              ● take(n)：返回一个包含数据集前n个元素的集合。
 *              ● first：效果等同于take(1),返回数据集中的第一个元素。
 *              ● foreach：循环遍历数据集中的每个元素，运行相应的逻辑。
 *              ● collect：将计算结果回收到Driver端。
 * @author hadoop
 * @date 2022/8/15 10:39
 */
object 行动算子 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc=new SparkContext(conf)

    val rdd:RDD[Int]=sc.makeRDD(List(1,2,3,4))
    //1、reduce:聚合数据
    //聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据。
    val rdd1:Int=rdd.reduce(_+_)
    println(rdd1)
    println

    //2、collect:收集数据到Driver
    //在驱动程序中，以数组 Array 的形式返回数据集的所有元素
    rdd.collect().foreach(println)
    println

    //3、count
    //返回 RDD 中元素的个数。
    val rdd2:Long=rdd.count()
    println(rdd2)
    println

    //4、 first
    //返回 RDD 中的第一个元素。
    val rdd3=rdd.first()
    println(rdd3)
    println

    //5、 take
    //返回一个由 RDD 的前 n 个元素组成的数组
    val rdd4=rdd.take(2)
    println(rdd4.mkString(","))
    println

    //6、takeOrdered
    //返回该 RDD 排序后的前 n 个元素组成的数组。
    val rdd5=rdd.takeOrdered(2)
    println(rdd5.mkString(","))
    println

    //7、aggregate
    //分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
    val rdd6=sc.makeRDD(List(1,2,3,4),2)
    val rdd7=rdd6.aggregate(10)(_+_,_+_)
    val rdd8=rdd6.aggregate(0)(_+_,_+_)
    println(rdd7)
    println(rdd8)
    println

    //8、countByKey
    //统计每种 key 的个数。
    val rdd9:RDD[(Int,String)]=sc.makeRDD(List((1,"a"),(1,"a"),(1,"a"),(1,"c"),(3,"c")))
    val rdd10:collection.Map[Int,Long]=rdd9.countByKey()
    println(rdd10)
    println

    //9.save 相关算子
    //将数据保存到不同格式的文件中。
    //保存成Text
    rdd.saveAsTextFile("e:/temp/a.txt")
    //序列化成对象保存到文件
    rdd.saveAsObjectFile("e:/temp/b.txt")
    //保存成Sequencefile文件
    rdd.map((_,1)).saveAsSequenceFile("e:/temp/c.txt")

    //分布式遍历 RDD 中的每一个元素，调用指定函数。
    rdd.map(num=>num).collect().foreach(println)
    println
    rdd.foreach(println)

    sc.stop()

  }

}

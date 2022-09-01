package org.example

object Array1 {

  def main(args: Array[String]): Unit = {
    Fixed
    Variable
    Traverse
    Operations

  }
  /*
   定长数组:长度不可变,元素可变
  语法:val/var 变量名=new Array[元素类型](数组长度)或
  val/var 变量名=Array(元素1,元素2...)
   */
  def Fixed={
    val arr = new Array[Int](5)
    arr(0)=20
    println(arr(0))

    val arr1 = Array("scala", "hadoop")
    println(arr1(0)+"--"+arr1.length)
  }

  /*
  变长数组:长度可变,可以添加,删除元素
  需要提前导入ArrayBuffer类:import scala.collection.mutable.ArrayBuffer
  语法:
  val/var 变量名=ArrayBuffer[元素类型]()
  val/var 变量名=ArrayBuffer(元素1,元素2...)
  += 添加元素
  -= 删除元素
  ++= 追加一个数组到变长数组
   */
  def Variable={
    import scala.collection.mutable.ArrayBuffer
    var arr1=ArrayBuffer("scala")
    println(arr1.length)
    arr1++=Array("spark")
    println(arr1.length)
  }

  /*
  数组遍历:
  1.for表达式直接遍历数组中的元素
  2.索引遍历数组中的元素
   */
  def Traverse={
    //直接基于数组操作当前的for循环是增强的for循环
    import scala.collection.mutable.ArrayBuffer
    val arr1 = ArrayBuffer("scala", "hadoop", "hive")
    for(i<-arr1){
      println(i)
    }

    /*注意:
    0 until n 区间[0,n)
    0 to n 区间[0,n]
     */
    val arr = Array("scala", "hadoop")
    for (i<-0 until arr.length){
      println(arr(i))
    }
  }

  /*
  数组操作
  1.sum
  2.sum
  3.max
  4.sorted
   */
  def Operations={
    //升序
    val arr = Array(12, 3, 17, 2, 22, 15)
    arr.sorted.foreach(println)





    //降序,reverse对数组进行反转
    val arr1=Array(12,3,17,2,22,15)
    arr1.sorted.reverse.foreach(println)

  }



}

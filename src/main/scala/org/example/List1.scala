package org.example

/**
 * scala中List列表可以保存重复的值,并且有先后顺序,列表分为可变和不可变列表
 */
object List1 {
  def main(args: Array[String]): Unit = {
    Fixed
    Variable

  }
  def Fixed={
    //不可变列表:元素和长度都不可变
    //val/var 变量名=List(元素1,元素2...)

    //案例一:基于Nil创建一个不可变的空列表
    val ny = Nil
    println(ny)
    //案例二:基于::创建列表,必须在后面添加Nil,元素1,2,3,4,5
    val mu = 1 :: 2 :: 3 :: 4 :: 5 :: Nil
    println(mu)
    //案例三:
    val nu=List(1,2,3,4,5)
    println(nu)
  }
  def Variable={
    /*
    元素和长度可变,在使用前需要导入import scala.collection.mutable.ListBuffer
    可变集合都在mutable包中
    语法:
    val/var 变量名=ListBuffer[Int]()
    val/var 变量名=ListBuffer(元素1,元素2...)
     */
    //案例一:创建列表,包含元素1,2,3,4,5
    import scala.collection.mutable.ListBuffer
    val alist=ListBuffer[Int](1,2,3,4,5)
    //获取第一个元素
    println(alist(0))
    //获取第二个元素
    println(alist(1))

    //追加一个新的元素
    val  a=alist+=23
    println(a)
    //追加一个列表,列表元素为9,10,11
    val b=alist++=List(9,10,11)
    println(b)
    //删除元素4
    val c=alist-=4
    println(c)
    //将可变列表转化为数组
    val d=alist.toArray
    d.foreach(println)
    //将可变列表转化为不可变列表
    val e=alist.toList
    val f=alist-=1
    println(e)
    println(f)
  }
}

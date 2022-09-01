package org.example

import scala.language.postfixOps

object Cycle {
  def main(args: Array[String]): Unit = {
    //for循环
    For()

    //嵌套循环
    Nested()

    //守卫：for表达式中，可以添加if语句进行筛选执行，这个if判断就称之为守卫
    Guard()

    //for嵌套+if守卫
    ForGuard()

    //for倒推式
    ForBack()

    //While循环
    While()

    //do...while
    DoWhile()

    Break()
  }

  def For(): Unit ={
    val nummbers = 1.to(20)
    for (i <- nummbers) {
      println(i)
    }
  }

  def Nested(): Unit ={
    for(i<-1 to 3;j<-1 to 4){
      print("*")
      //当j=4，即到达每一行的末尾，换行
      if(j==4)
        println
    }
  }

  def Guard(): Unit ={
    for (i <- 0 to 20 if i%2==0){
      println(i)
    }
  }

  //for嵌套+if守卫
  //5行5列中只打印行和列中的奇数
  def ForGuard(): Unit ={
    for(i<- 1 to 5 if i%2==1;j<- 1 to 5 if j%2==1){
      println(i+"-"+j+" ")
      if(j==5)
        println(" ")
    }
  }

  //for倒推式
  /*
  使用集合接收每一次循环体的结果
  生成元素为10 20 30...200的集合
  */
  def ForBack(): Unit ={
    val coll=for(i<- 1 to 20)yield i*10
    println(coll)
  }

  //while循环,和java一致
  def While(): Unit ={
    var sum=0
    var i=0
    while(i<=100){
      sum+=i
      i+=1
    }
    println(sum)
  }

  //do...While
  def DoWhile(): Unit ={
    var sum=0
    var i=0
    do{
      sum=sum+i
      i=i+1
    }while(i<=100)
    println(sum)
  }

  /*
  在scala里没有break/continue,需要引入scala.util.control包的Break类的breable和break方法
   */
  //打印1到20之间的数字,如果为10则退出循环
  def Break(): Unit ={
    import scala.util.control.Breaks._
    breakable{
      for(i <-1 to 20){
        if(i==10)break()
        else println(i)
      }
    }

  }



}

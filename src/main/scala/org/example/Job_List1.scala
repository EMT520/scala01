package org.example

/**
*将字符串:"This is a scala -1 Hello scala -1 Hello"
* *转换成:
* List((This,1),(is,1),(a,1),(scala,1),(Hello,1),(scala,1),(Hello,1))
*/
object Job_List1 {
  def main(args: Array[String]): Unit = {
    val a="This is a scala -1 Hello scala -1 Hello"
    val b= a.split("\\s+")
      .toList.filter(_!="-1")
      .map((_,1)).groupBy(_._1).map(x=>(x._1->x._2.size)).toList
    println(b)
  }

}

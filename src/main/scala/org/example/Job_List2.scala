package org.example

//List转换
/**
*将2个集合:
*List(("a",10),("b",22),("c",12))
 * List(("a",20),("b",12),("d",20))
*转换成: List((b,34),(a,30)，(d, 20),(c,12))
 * 转换过程:
* ListBuffer((a,10)，(b,22)，(c,12)，(a, 20)，(b,12)，(d,20))
*Map(a -> ListBuffer((a,10),(a,20)),b ->ListBuffer((b,22)，(b,12))
 * ,c ->ListBuffer((c,12)),d -> ListBuffer ((d,20)))
* List((a, 30)，(b,34)，(c,12)，(d,20))* List((b,34), (a,30)，(d, 20)，(c,12))*/

object Job_List2 {
  def main(args: Array[String]): Unit = {
    val list1=List(("a",10),("b",22),("c",12))
    val list2=List(("a",20),("b",12),("d",20))
    //val list3=list1.union(list2)
    //val list3=ListBuffer.concat(list1,list2)
    val list3=list1++list2
    println(list3)
    //分组
    //val list4=list3.groupBy(_._1).map(x=>{
    //      (x._1,x._2.map(_._2).reduce(_+_))
    val list4=list3.groupBy(_._1).map(x=>{
      (x._1,x._2.map(_._2).sum)
    }).toList
    println(list4)
    //排序
    val list5=list4.sortWith(_._2>_._2)
    //val list5=list4.sortBy(_._2).reverse
    println(list5)
  }




}

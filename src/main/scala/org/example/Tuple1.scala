package org.example

/**
 * 元组可以用来包含一组不同类型的值
 * 语法:
 * val/var 元组=(元素1,元素2...)
 * 或
 * val/var 元组=元素1->元素2
 * 注意:箭头定义元组,元组只有两个元素
 * 使用_1,_2,_3,_4...来访问元组中的元素,_1表示访问第一个元素,以此类推
 * 元组中的元素不能修改
 */
object Tuple1 {
  def main(args: Array[String]): Unit = {
    val stu=(1,"lisi",30,"18207")
    println(stu._2)

    val teacher="zhangsan"->33
    println(teacher._1)

  }


}

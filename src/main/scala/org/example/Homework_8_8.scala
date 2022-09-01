package org.example

//递归

object Homework_8_8 {
  def main(args: Array[String]): Unit = {
    println(Job1(2))
    println(Job2("hello"))
  }
  /**
   * 有兄弟5个人，问第老大年龄，答比老二大2岁，问老二的年龄，答比老三大2岁
   * 问老三年龄，答比老四大2岁，问老四年龄，答比老五大2岁，问老五年龄，答10岁
   * 求老二的年龄
   */
  def Job1(n: Int): Int = {
    if (n == 5) 10
    else Job1(n + 1) + 2
  }
  /**
   * 将一个字符串反转
   * 当字符串长度小于等于1时,反转的字符串等于它本身
   * 当字符串长度大于1时,(1)运用substring截取最后一个字符,charAt返回剩下的字符 (2)charAt返回第一个字符放在末尾 (3)调用head和tail方法
   */
  def Job2(Str: String): String = {
    if (Str.length <= 1) return Str
    Job2(Str.tail)+Str.head
    //reverse(Str.substring(1)) + Str.charAt(0)
    //Str.charAt(Str.length() - 1) + Job2(Str.substring(0, Str.length() - 1))

    /*
    head 表示数组的第一个元素
    tail 表示数组除去 head 后的数组
    last 表示数组的最后一个元素
    init 表示数组除去 last 后的数组
     */
  }

}

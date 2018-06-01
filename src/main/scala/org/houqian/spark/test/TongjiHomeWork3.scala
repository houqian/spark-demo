package org.houqian.spark.test

import scala.collection.mutable
import scala.io.Source

/**
  *
  * @author : houqian
  * @since : 2018/5/31
  * @version : 1.0
  */
object TongjiHomeWork3 {

  /**
    * 第二期作业讨论：
    *
    * 为了提升大家的「数据工程能力」，我在业务实践的过程中找到了这个需求，大家可以实践一下，验证下自己的工程能力。
    *
    * 一、问题背景
    *
    * "现有一批理财用户的APP安装列表，业务人员想通过「关联分析」去寻找"APP应用"与"投资次数"的关系。不过在此之前，需要先统计出不同"APP安装组合"的人数分布？"
    *
    * username / applist
    * 张三    作业帮,掌上办证照,优酷,QQ音乐,国寿e宝,招商银行,微信,陆金所,华为游戏中心,WiFi管家,手机淘宝,支付宝
    * 李四    小牛在线,掌上办证照,爱奇艺,趣头条,中国农业银行,美团,蜻蜓FM,爱钱进,微信,应用宝,悦动圈,百度
    * 王二    小牛在线,支付宝,中信证券高端版,优酷,美团,微信,中国农业银行,陆金所,互传,作业帮,百度,手机淘宝
    * 麻子    作业帮,多盈理财,搜狗输入法,中国农业银行,盈盈理财,国寿e宝,爱钱进,人人贷理财,微信,向上金服,互传
    * 张飞    ofo共享单车,WiFi管家,蜻蜓FM,爱奇艺,趣头条,优酷,华为应用市场,QQ音乐,华为钱包,应用宝,悦动圈,微信
    * 李飞    转转,盈盈理财,人人贷理财,微信,积木盒子,WiFi管家,百度地图,QQ音乐,华为游戏中心,支付宝,中国移动
    * 王三    美团,微信,中国农业银行,爱钱进,人人贷理财,向上金服,互传,QQ音乐,华为游戏中心
    *
    * 二、结果概览
    *
    * app1:app2    3
    * ...
    * app9:app8    8
    *
    * 三、温馨提示
    *
    *01. 由于线上的用户量级比较大，因此不考虑Excel来处理；
    *02. 对于"app1:app2"与"app2:app1"，看作是同一组合；
    *03. 如果是三个app组合(app1:app2:app3)，那又如何统计；
    *
    * 大家可以从「理论」和「实战」的角度去展开讨论。
    */

  def main(args: Array[String]): Unit = {

    val lines: List[String] = getLinesFromClassPathFile("username_applist.txt")

    val appUsernameMap = new mutable.HashMap[String, mutable.HashSet[String]]()

    val appSet = lines map {
      line =>
        val lineArr = line.split("    ")
        val username = lineArr(0)

        val applist = lineArr(1)
        val applistSet = applist.split(",").toSet

        applistSet map {
          app =>
            if (appUsernameMap.contains(app)) {
              appUsernameMap(app).add(username)
            }
            else {
              val usernameSet = new mutable.HashSet[String]()
              usernameSet.add(username)
              appUsernameMap.put(app, usernameSet)
            }
        }
        applistSet
    } reduce { (x, y) => x ++ y }

    //    println("appUsernameMap:" + appUsernameMap)
    //    println("appUsernameMap" + appSet)

    val appDistinctList = appSet.toList
    val result1Map = new mutable.HashMap[String, String]()
    appDistinctList foreach {
      a1 =>
        appDistinctList foreach {
          a2 =>
            if (a1 != a2) {
              val usernameSet1: mutable.HashSet[String] = appUsernameMap(a1)
              val usernameSet2: mutable.HashSet[String] = appUsernameMap(a2)
              val count = usernameSet1.intersect(usernameSet2).size
              if (count > 0) {
                val resultLine = s"$a1:$a2 $count"
                val key = appDistinctList.indexOf(a1) + appDistinctList.indexOf(a2)
                result1Map.put(key.toString, resultLine)
              }
            }
        }
    }

    val result1 = result1Map.values
    println("\n\n======两个=====")
    println(result1.mkString("\n"))

    val result2Map = new mutable.HashMap[String, String]()
    appDistinctList foreach {
      a1 =>
        appDistinctList foreach {
          a2 =>
            appDistinctList foreach {
              a3 =>
                if (a1 != a2 && a2 != a3 && a1 != a3) {
                  val usernameSet1: mutable.HashSet[String] = appUsernameMap(a1)
                  val usernameSet2: mutable.HashSet[String] = appUsernameMap(a2)
                  val usernameSet3: mutable.HashSet[String] = appUsernameMap(a3)

                  val count = usernameSet1.intersect(usernameSet2).intersect(usernameSet3).size
                  if (count > 0) {
                    val resultLine = s"$a1:$a2:$a3 $count"
                    val key = appDistinctList.indexOf(a1) + appDistinctList.indexOf(a2) + appDistinctList.indexOf(a3)
                    result2Map.put(key.toString, resultLine)
                  }
                }
            }
        }
    }
    val result2 = result2Map.values
    println("======三个=====")
    println(result2.mkString("\n"))

  }

  private def getLinesFromClassPathFile(fileName: String) = {
    val fileStream = getClass.getClassLoader.getResourceAsStream(fileName)
    val lines = Source.fromInputStream(fileStream).getLines().toList
    lines
  }
}

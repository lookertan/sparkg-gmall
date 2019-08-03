package com.tanchuang.gmall.acc

import com.tanchuang.sparkmall.common.util.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

class MyAcc() extends AccumulatorV2[UserVisitAction,Map[String,(Long,Long,Long)]]{
  var map=Map[String,(Long,Long,Long)]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]] = {
    val acc = new MyAcc()
//    acc.map=this.map
    acc.map=this.map
    acc
  }

  override def reset(): Unit = {
    map=Map[String,(Long,Long,Long)]()
  }

  override def add(v: UserVisitAction): Unit = {
    if (v.click_category_id != -1){//点击行为
      val (clickCount,orderCount,payCount) = map.getOrElse(v.click_category_id+"",(0L,0L,0L))
      map+= v.click_category_id+""->(clickCount+1,orderCount,payCount)
   }else if(v.order_category_ids !=null){//订单行为
      val categoryArray = v.order_category_ids.split(",")//所有的品类
      categoryArray.foreach{
        case x =>
          val (clickCount,orderCount,payCount) = map.getOrElse(x,(0L,0L,0L))
          map += x->(clickCount,orderCount+1,payCount)
      }
    }else if(v.pay_category_ids!=null){//支付行为
      val categoryArray = v.pay_category_ids.split(",")//所有的品类
      categoryArray.foreach{
        case x =>
          val (clickCount,orderCount,payCount) = map.getOrElse(x,(0L,0L,0L))
          map += x->(clickCount,orderCount,payCount+1)
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]]): Unit = {
    other match {
      case o :MyAcc=> o.map.foreach{
        case (categoryId,(other_clickCount,other_orderCount,other_payCount))=> {

          val (clickCount,orderCount,payCount) = this.map.getOrElse(categoryId,(0L,0L,0L))
          map+= categoryId->(clickCount+other_clickCount,orderCount+other_orderCount,payCount+other_payCount)
        }
      }
    }
  }

  override def value: Map[String, (Long, Long, Long)] = map
}

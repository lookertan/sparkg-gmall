package com.tanchuang.exercise

import scala.util.control.Breaks._

/**
 * 快排训练
 */
object QuickSort {
  def main(args: Array[String]): Unit = {
    val array: Array[Int] = List(15, 7, 6, 8, 16).toArray

    myQuickSort(0, array.length - 1, array)

    array.foreach(println)

  }


  def myQuickSort(left: Int, right: Int, arr: Array[Int]): Unit = {

    var l = left
    var r = right
    val pivot = arr((left + right) / 2)
    var temp = 0
    //解决单次 排序问题
    breakable {
      while (l < r) {

        while (arr(l) < pivot) {
          l += 1
        }

        while (arr(r) > pivot) {
          r -= 1
        }

        if (l >= r) {
          break()
        }

        temp = arr(l)
        arr(l) = arr(r)
        arr(r) = temp
      }

    }

    if (l == r) {
      l += 1
      r -= 1
    }

    if (left < r) {
      myQuickSort(left, r, arr)
    }

    if (right > l) {
      myQuickSort(l, right, arr)
    }

  }
}

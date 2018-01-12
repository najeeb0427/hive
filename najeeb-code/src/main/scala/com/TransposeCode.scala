package com
import org.apache.spark._

object Transpose {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkKafkaPOC")
    val sc = new SparkContext(conf)
  }
}

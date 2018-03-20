package com.jacoffee.codebase.spark.core

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

/**
  * reference from org.apache.spark.rdd.ReliableRDDCheckpointData
*/
object DeleteCheckpoint {

  private def cleanCheckpoint(sc: SparkContext, dir: String) = {
    val path = new Path(dir)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        println(s"Error deleting ${path.toString()}")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("X").setMaster("local[1]")
    val sc = new SparkContext(conf)
    cleanCheckpoint(sc, "hdfs://localhost/user/allen/streaming/wordcount")
  }

}

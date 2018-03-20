package com.jacoffee.codebase.spark.utils

import com.jacoffee.codebase.utils.HadoopUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, Duration}
import org.slf4j.LoggerFactory
import scala.concurrent.duration.{Duration => ScalaDuration}

object SparkUtils {

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  val CP_DIR = "spark.streaming.checkpoint.directory"
  val STATE_SNAPSHOT_DIR = "spark.streaming.stateful.snapshot.dir"
  val DURATION = "spark.streaming.duration"

  val DEFAULT_CHECKPOINT_DURATION_MULTIPLIER = 5
  val STATE_SNAPSHOT_PARTITION = 5

  implicit def scalaDurationToSparkDuration(duration: ScalaDuration): Duration = Duration(duration.toMillis)

  def periodicSnapShotDump[K, StateType](
    rdd: RDD[(K, StateType)], currentTime: Time
  ): Unit = {

    HadoopUtils.safeDelete("hdfs://localhost/user/allen/streaming/wordcount-snapshot")
    rdd.repartition(STATE_SNAPSHOT_PARTITION).saveAsObjectFile("hdfs://localhost/user/allen/streaming/wordcount-snapshot")

    /*
    val sparkConf = rdd.context.getConf
    val snapShotDir = sparkConf.get(STATE_SNAPSHOT_DIR)
    val duration =  ScalaDuration(sparkConf.get(DURATION))

    // save at every five duration
    if (currentTime.isMultipleOf(duration * DEFAULT_CHECKPOINT_DURATION_MULTIPLIER)) {
      HadoopUtils.safeDelete(snapShotDir)
      // cleanCheckpoint(rdd.context, snapShotDir)
      logger.info(s"Periodic checkpointing data into ${snapShotDir}")
      rdd.repartition(STATE_SNAPSHOT_PARTITION).saveAsObjectFile(snapShotDir)
    }
    */
  }


  def cleanCheckpoint(sc: SparkContext, dir: String) = {
    val path = new Path(dir)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        logger.info(s"Error deleting ${path.toString()}")
      }
    }
  }

}

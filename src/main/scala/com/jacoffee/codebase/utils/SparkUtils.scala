package com.jacoffee.codebase.spark.utils

import org.joda.time.DateTime
import org.apache.hadoop.fs.Path
import com.google.common.base.Stopwatch
import java.util.concurrent.TimeUnit
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, StateSpec, Duration}
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.reflect.ClassTag

object SparkUtils {

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  val CP_DIR = "spark.streaming.checkpoint.directory"
  val STATE_SNAPSHOT_DIR = "spark.streaming.snapshot.dir"
  // STATE_SNAPSHOT_CP_DURATION must be multiple of DURATION
  val DURATION = "spark.streaming.duration"
  val STATE_SNAPSHOT_CP_DURATION = "spark.streaming.snapshot.duration"
  val STATE_SNAPSHOT_PARTITION = "spark.streaming.snapshot.partition"
  val DEFAULT_STATE_SNAPSHOT_PARTITION = 5

  implicit def scalaDurationToSparkDuration(duration: ScalaDuration): Duration = Duration(duration.toMillis)

  /*
    HadoopUtils.safeDelete("hdfs://localhost/user/allen/streaming/wordcount-snapshot")
    rdd.repartition(STATE_SNAPSHOT_PARTITION).saveAsObjectFile("hdfs://localhost/user/allen/streaming/wordcount-snapshot")
  */
  def periodicSnapShotDump[T](
    rdd: RDD[T], startTime: Long, currentTime: Long
  ): Unit = {
    val sparkConf = rdd.context.getConf

    for {
      snapShotDir <- sparkConf.getOption(STATE_SNAPSHOT_DIR)
      snapShotDuration <- sparkConf.getOption(STATE_SNAPSHOT_CP_DURATION)
    } yield {
      val partitionNumber = sparkConf.getInt(STATE_SNAPSHOT_PARTITION, DEFAULT_STATE_SNAPSHOT_PARTITION)

      if (currentTime != startTime && (currentTime - startTime) % ScalaDuration(snapShotDuration).toMillis == 0) {
        cleanCheckpoint(rdd.context, snapShotDir)
        logger.info(s"Periodic checkpointing data into ${snapShotDir}")
        rdd.repartition(partitionNumber).saveAsObjectFile(snapShotDir)
      }
    }
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

  implicit class PimpedDStream[K: ClassTag, V: ClassTag](dstream: DStream[(K, V)]) extends Serializable {

    private var zeroTime: Time = null
    // save snapshots at interval
    def foreachStateRDD[StateType: ClassTag, MappedType: ClassTag](
      spec: StateSpec[K, V, StateType, MappedType],
      rddHandler: (RDD[(K, StateType)], Time) => Unit
    ): Unit = {
      val stateSnapshots = dstream.mapWithState(spec).stateSnapshots()
      stateSnapshots.foreachRDD { (rdd, time) =>

        if (zeroTime == null) {
          zeroTime = time
        }
        periodicSnapShotDump(rdd, zeroTime.milliseconds, time.milliseconds)

        val stopwatch = new Stopwatch().start()
        rddHandler(rdd, time)
        stopwatch.stop()
        val millis = stopwatch.elapsed(TimeUnit.MILLISECONDS)

        logger.info(
          s"Finish streaming rdd handling in batch {}, completed in {}",
          List(new DateTime(time.milliseconds).toString("yyyy-MM-dd HH:mm:ss"), millis.toString): _*
        )
      }
    }
  }

}

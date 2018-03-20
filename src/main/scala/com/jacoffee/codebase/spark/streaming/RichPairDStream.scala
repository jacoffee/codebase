package com.jacoffee.codebase.spark.streaming

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.OffsetRange

import scala.reflect.ClassTag

class RichPairDStream[K: ClassTag, V: ClassTag, V2: ClassTag](self: DStream[(K, V)]) extends Serializable {


}

package com.jacoffee.codebase.utils

import java.io.IOException
import java.net.{URI, URISyntaxException}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path, FileSystem}

/**
  * Created by allen on 3/18/18.
  */
object HadoopUtils {

  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def getFileSystem(uri: String): FileSystem = {
    getFileSystem(URI.create(uri))
  }

  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def getFileSystem(uri: URI): FileSystem = {
    val conf: Configuration = new Configuration
    FileSystem.get(uri, conf)
  }

  @throws(classOf[IOException])
  @throws(classOf[IllegalArgumentException])
  def mkdirs(path: String): Boolean = {
    val fs: FileSystem = getFileSystem(path)
    fs.mkdirs(new Path(path))
  }

  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def existsPath(path: String): Boolean = {
    val fs: FileSystem = getFileSystem(path)
    existsPath(fs, path)
  }

  @throws(classOf[IOException])
  private def existsPath(fs: FileSystem, path: String): Boolean = {
    val status = Option(fs.globStatus(new Path(path)))
    status.nonEmpty && status.exists(_.size > 0)
  }

  /**
    * @param uri  hdfs path hdfs://localhost/xxx
    * @throws java.io.IOException
    * @throws java.net.URISyntaxException
    * @return the file size or total file size in directory if uri is a directory
    */
  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def getFileSize(uri: String) = {
    val path = new Path(uri)
    getFileSystem(uri).getContentSummary(path).getLength
  }

  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def getInputStream(path: String): FSDataInputStream = {
    val fs = getFileSystem(path)
    val fsDataInputStream = fs.open(new Path(path))
    fsDataInputStream
  }

  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  private def deletePath(uri: String, recursive: Boolean): Boolean = {
    deletePath(new Path(uri), recursive)
  }

  private def deletePath(path: Path, recursive: Boolean): Boolean = {
    val fs: FileSystem = getFileSystem(path.toUri)
    // logger.info(s"Delete hdfs path ${path}")
    fs.delete(path, recursive)
  }

  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def safeDelete(uri: String, recursive: Boolean = true) {
    if (existsPath(uri)) {
      deletePath(uri, recursive)
    }
  }

}

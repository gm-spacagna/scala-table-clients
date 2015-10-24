package com.github.gmspacagna.scalatable.clients

import java.io._

import com.twitter.logging.Logger
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{FileSystem, _}
import org.apache.hadoop.io.compress.CompressionCodecFactory

import scala.io.Source

abstract class AbstractFSClient {

  def getLines(path: String): Iterable[String] = {
    iterator(path).toIterable
  }

  def getLines(parent: String, child1: String, child2: String): Iterable[String] = {
    getLines(composeAbsPath(parent, List(child1, child2)))
  }

  def scanLines(path: String, cb: (String => Any)) {
    for (line <- iterator(path)) {
      cb(line)
    }
  }

  // Safe version of scanLines
  def scanOrElse(path: String, cb: (String) => Any, failure: (FileNotFoundException => Any) = (_) => Nil): Unit = {
    try {
      scanLines(path, cb)
    } catch {
      case e: FileNotFoundException =>
        Logger.get().error("No available data in the path " + path)
        failure(e)
    }
  }

  def countLines(path: String): Long = {
    var n = 0l
    def cb(line: String) = n += 1
    scanOrElse(path, cb)
    n
  }

  def iterator(path: String): Iterator[String]

  def iterator(parent: String, child: String): Iterator[String] = {
    iterator(composeAbsPath(parent, child))
  }

  def composeAbsPath(parent: String, child: String): String = {
    require(parent.startsWith("/"), "The parent path " + parent + " must specify an absolute path")

    val relChildPath = if (child.startsWith("/")) child.substring(1, child.length()) else child

    if (parent.endsWith("/")) {
      parent + relChildPath
    } else {
      parent + "/" + relChildPath
    }
  }

  def composeAbsPath(parent: String, children: Iterable[String]): String = {
    children.toList match {
      case Nil => parent
      case head :: Nil => composeAbsPath(parent, head)
      case head :: tail => composeAbsPath(composeAbsPath(parent, head), tail)
    }
  }

  def composeAbsPath(parent: String, child1: String, child2: String): String =
    composeAbsPath(parent, List(child1, child2))
}

object HDFSClient extends AbstractFSClient {
  val conf = new Configuration()
  Logger.get().info("HDFS configuration properties: " + conf.iterator())

  override def iterator(path: String): Iterator[String] = {
    readLines(new Path(path), conf)
  }

  private def readLines(location: Path, conf: Configuration): Iterator[String] = {
    val fileSystem = FileSystem.get(location.toUri, conf)
    val factory = new CompressionCodecFactory(conf)
    val items = fileSystem.listStatus(location)
    if (items == null) return Iterator.empty
    val results = for (item <- items if !item.getPath.getName.startsWith("_")) yield {

      // check if we have a compression codec we need to use
      val codec = factory.getCodec(item.getPath)

      val stream = if (codec != null) {
        codec.createInputStream(fileSystem.open(item.getPath))
      } else {
        fileSystem.open(item.getPath)
      }

      val writer = new StringWriter()
      IOUtils.copy(stream, writer, "UTF-8")
      val raw = writer.toString
      raw.split("\n")
    }
    results.flatten.toIterator
  }
}

/**
 * A client to a file accessed via an NFS mount.
 * The path must be specified relatively to the NFS mount location.
 */
case class NFSClient(nfsMountPath: String, newLine: String = "\n") extends AbstractFSClient {

  override def iterator(path: String): Iterator[String] = iterator(new File(composeAbsPath(nfsMountPath, path)))

  def iterator(path: File): Iterator[String] =
    if (path.isDirectory) iteratorDirectoryFiles(path)
    else iteratorSingleFile(path)

  def iteratorSingleFile(file: File): Iterator[String] = {
    val result = for (bs <- resource.managed(Source.fromFile(file))) yield bs.iter.mkString
    val string = result.acquireFor(identity).fold(fa => throw fa.head, v => v)

    val lines = string.split(newLine).filterNot(_.isEmpty)
    lines.toIterator
  }

  def iteratorDirectoryFiles(directory: File): Iterator[String] = {
    require(directory.exists(), "Unexisting path " + directory.getAbsolutePath)

    val files = directory.listFiles filter (file => file.isFile && !file.getName.startsWith("_"))

    iteratorDirectoryFilesSequential(files.toIterator)
  }

  def iteratorDirectoryFilesInMemory(files: Array[File]): Iterator[String] = {
    val iterators = files.map(iteratorSingleFile)
    iterators.reduce(_ ++ _)
  }

  def iteratorDirectoryFilesSequential(files: Iterator[File]): Iterator[String] =
    new DirectoryLinesIterator(files)

  class DirectoryLinesIterator(files: Iterator[File]) extends Iterator[String] {
    require(files.nonEmpty)

    private var currentIterator: Iterator[String] = Iterator.empty
    private var cachedNext: String = loadNext()

    override def hasNext: Boolean = cachedNext != null

    override def next(): String = {
      val res = cachedNext
      cachedNext = loadNext()
      res
    }

    private def loadNext(): String = {
      if (currentIterator.hasNext) {
        currentIterator.next()
      } else if (files.hasNext) {
        currentIterator = iteratorSingleFile(files.next())
        loadNext()
      } else {
        null
      }
    }
  }
}

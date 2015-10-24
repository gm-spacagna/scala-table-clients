package com.github.gmspacagna.scalatable.clients

import java.sql.{Connection, DriverManager, ResultSet}

import com.twitter.logging.Logger

import scala.Array.canBuildFrom

class HiveClient(hiveServer: String, port: String = 10000) {
  val DriverName = "org.apache.hadoop.hive.jdbc.HiveDriver"
  val FetchSize = 10000
  try {
    Class.forName(DriverName)
  } catch {
    case e: ClassNotFoundException =>
      throw new RuntimeException("The HIVE Driver is not available in the class path")
  }

  private def openConnection(): Connection = {
    val url = "jdbc:hive://" + hiveServer + ":" + port + "/default"
    Logger.get().info("Opening connection to " + url)
    DriverManager.getConnection(url, "", "")
  }

  def executeQuery(sql: String, cb: ((ResultSet) => Unit)): Unit = {
    executeQuery(sql, (res: ResultSet, acc: Option[Unit]) => cb(res))
  }

  def executeQuery[T](sql: String, cb: ((ResultSet, Option[T]) => T)): Option[T] = {
    val con = openConnection()
    Logger.get().info("Running HQL query: " + sql)
    val stmt = con.createStatement()
    stmt.setFetchSize(FetchSize)
    Logger.get().info("Fetch size set to: " + FetchSize)
    val res = stmt.executeQuery(sql)
    Logger.get().info("Query executed, returning results...")
    var i = 0
    var result: Option[T] = None
    val first = res.next()
    if (!first) {
      throw new IllegalStateException("No any result for the hive query: " + sql)
    } else {
      do {
        result = Option(cb(res, result))
        i += 1
        if (i % 1000 == 0) Logger.get().info("Returned " + i + " results")
      } while (res.next())

      Logger.get().info("Total returned results: " + i)
      res.close()
      stmt.close()
      Logger.get().info("Closing connection...")
      con.close()
      result
    }
  }

  def executeQuery[E](sql: String, t: (ResultSet) => E): Iterable[E] =
    executeQuery(sql, (res: ResultSet, acc: Option[List[E]]) =>
      acc match {
        case Some(list) => list :+ t(res)
        case None => List[E](t(res))
      }).get
}

object HiveUtils {
  def extractArrayElems(str: String): Array[String] = extractArrayElems(str, ",")

  def extractArrayElems(str: String, delim: String): Array[String] =
    str.replaceAll("(\\[)|(\\])", "").split(delim).map(_.trim())
}

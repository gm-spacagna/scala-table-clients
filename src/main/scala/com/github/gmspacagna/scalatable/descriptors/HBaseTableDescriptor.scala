package com.github.gmspacagna.scalatable.descriptors

abstract case class HBaseTableDescriptor(path: String, family: String)

abstract class HBaseTableColumn(val qualifier: String) {
  override def toString = qualifier
}

abstract class HBaseTableFields[T <: HBaseTableColumn](map: Map[String, T]) {
  def getField(qualifier: String): Option[T] = if (map.contains(qualifier)) Some(map(qualifier)) else None

  def isColumnOf(qualifier: String, field: HBaseTableColumn) = map.getOrElse(qualifier, None) == field

  def getFirstPrefixMatchingField(prefix: String) = {
    val list = map.filterKeys(_.startsWith(prefix)).toList
    if (list.isEmpty) None else list(0)
  }
}

trait TableColumnImplicits {
  implicit def qualifier2String(col: HBaseTableColumn): String = col.qualifier
}

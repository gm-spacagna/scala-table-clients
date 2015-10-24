package com.github.gmspacagna.scalatable.descriptors

trait HiveTableDescriptor extends HiveTableOperators {
  def tableName: String
  def dbName: String
  def fullTableName = dbName + "." + tableName
}

trait HiveTableOperators {
  case object Select extends HQLOperator("SELECT")
  case object From extends HQLOperator("FROM")
  case object Where extends HQLOperator("WHERE")
  case object GroupBy extends HQLOperator("GROUP BY")
  case object Like extends HQLOperator("LIKE")
  case object Limit extends HQLOperator("LIMIT")
  case object LeftSemiJoin extends HQLOperator("LEFT SEMI JOIN")
  case object Join extends HQLOperator("JOIN")
  case object On extends HQLOperator("ON")
  case object And extends HQLOperator("AND")
  case object Partition extends HQLOperator("PARTITION")
  case object Table extends HQLOperator("TABLE")
  case object Insert extends HQLOperator("INSERT")
  case object As extends HQLOperator("AS")

  def CollectSet(arg: String) = new HQL1ArgOperator("collect_set", arg) {}
  def Count(arg: String) = new HQL1ArgOperator("COUNT", arg) {}
  def Distinct(arg: String) = new HQL1ArgOperator("DISTINCT", arg) {}
  def Sum(arg: String) = new HQL1ArgOperator("SUM", arg) {}

  case object WildChar extends SyntaxOperator("*")
  case object Space extends SyntaxOperator(" ")
  case object CommaSpace extends SyntaxOperator(", ")

  implicit def Operator2String(op: HQLOperator) = op.toString
  implicit def Operator2String(op: HQL1ArgOperator) = op.toString
  implicit def Operator2String(op: SyntaxOperator) = op.toString
  implicit def Operator2String(op: HiveTableColumn) = op.toString
  implicit def Operator2String(op: GenericId) = op.toString
  implicit def Descriptor2String(descriptor: HiveTableDescriptor) = descriptor.tableName

  def func(op: HQL1ArgOperator, arg: String): String = op + "(" + arg + ")"

  def fullColumnName(tableAlias: String, col: HiveTableColumn) = tableAlias + "." + col
}

object HiveTableOperators extends HiveTableOperators

abstract class HiveEnum(val name: String) {
  override def toString = name
}

abstract class GenericId(name: String) extends HiveEnum(name)

abstract class HiveTableColumn(name: String) extends HiveEnum(name)

abstract class HQLOperator(name: String) extends HiveEnum(name)

abstract class HQL1ArgOperator(name: String, arg: String) extends HiveEnum(name + "(" + arg + ")")

abstract class SyntaxOperator(name: String) extends HiveEnum(name)


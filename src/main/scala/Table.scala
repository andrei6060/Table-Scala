import util.Util.{Line, Row}

import scala.annotation.tailrec
import scala.collection.immutable.{List, Map}


trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    if(r.contains(colName))
      Some(predicate(r(colName)))
    else
      None
  }
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    if((f1.eval(r).isEmpty) || (f2.eval(r).isEmpty)) {
      None
    } else {
        if(f1.eval(r).get && f2.eval(r).get) {
          Some(true)
        } else
          Some(false)
    }
  }
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    if ((f1.eval(r).isEmpty) || (f2.eval(r).isEmpty)) {
      None
    } else {
      if (f1.eval(r).get || f2.eval(r).get)
        Some(true)
      else
        Some(false)
    }
  }
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Some(t)
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval.get.select(columns)
  }
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval.get.filter(condition)
  }
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] =
    Some(target.eval.get.newCol(name, defaultVal))
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = t1.eval.get.merge(key, t2.eval.get)
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames : Line = columnNames
  def getTabular : List[List[String]] = tabular

  // 1.1
  override def toString: String = {
    val nume = columnNames.mkString("",",","\n");
    val tabel = tabular.map(_.mkString("",",","")).mkString("\n");
    nume++tabel
  }

  // 2.1
  def select(columns: Line): Option[Table] = {
    val idk = getTabular.transpose
    val rez = columns.map( elem => if (getColumnNames.contains(elem)) idk(getColumnNames.indexOf(elem)) else return None)
    Some(new Table(columns, rez.transpose))
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    val rows = getTabular.map(el => columnNames.zip(el).toMap)
    val idk = rows.map(elem => cond.eval(elem))

    def functie(tabela: List[List[String]], adv: List[Option[Boolean]]): List[List[String]] = {
      @tailrec
      def functie2(tabela: List[List[String]], adv: List[Option[Boolean]], rez: List[List[String]], n: Int): List[List[String]] = {
        if (n == adv.length ) {

          rez
        } else if(adv(n).isDefined) {

          if (adv(n).get)
            functie2(tabela, adv, tabela(n) :: rez, n + 1)
          else {

            functie2(tabela, adv, rez, n + 1)
          }
        } else {
          functie2(tabela, adv, rez, n + 1)
        }
      }

      functie2(tabela, adv, Nil: List[List[String]], 0)
    }

    val body = functie(getTabular, idk).reverse

    if(body.nonEmpty)
      Some(new Table(columnNames, body))
    else
      None
    //idk(getColumnNames.size)
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table = {
    val newName = columnNames.appended(name)
    val newBody = getTabular.map(_.appended(defaultVal))
    new Table(newName, newBody)
  }

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = {
    def function(key: String, a: List[Map[String, String]], head: List[String]): List[Map[String, String]] = {

      @tailrec
      def merge(a: Map[String, String], b: Map[String, String], head: List[String], c: Map[String, String], idx: Int): Map[String, String] = {
        if (idx == head.length)
          c
        else {
          val mapA = a.get(head(idx))
          val mapB = b.get(head(idx))
          if (mapA.isDefined && (mapB.isDefined))
            if (mapA.get.equals(mapB.get)) {
              val sa_vedem: Map[String, String] = c ++ Map(head(idx) -> mapA.get)
              merge(a, b, head, sa_vedem, idx + 1)
            } else {
              val sir: String = mapA.get ++ ";"
              val sir2: String = sir ++ mapB.get
              merge(a, b, head, c ++ Map(head(idx) -> sir2), idx + 1)
            }
          else if (mapA.isEmpty && mapB.isDefined) {
            merge(a, b, head, c ++ Map(head(idx) -> mapB.get), idx + 1)
          } else if (mapB.isEmpty && mapA.isDefined) {
            merge(a, b, head, c ++ Map(head(idx) -> mapA.get), idx + 1)
          } else {
            merge(a, b, head, c ++ Map(head(idx) -> ""), idx + 1)
          }
        }
      }

      @tailrec
      def create(a: Map[String, String], head: List[String], c: Map[String, String], idx: Int): Map[String, String] = {
        if (idx == head.length)
          c
        else {
          val mapA = a.get(head(idx))
          if (mapA.isDefined) {
            val sa_vedem: Map[String, String] = c ++ Map(head(idx) -> mapA.get)
            create(a, head, sa_vedem, idx + 1)
          } else {
            val sa_vedem: Map[String, String] = c ++ Map(head(idx) -> "")
            create(a, head, sa_vedem, idx + 1)
          }
        }
      }


      @tailrec
      def find(a: Map[String, String], b: List[Map[String, String]], key: String, curr: Int): Int = {
        if (b.nonEmpty) {
          if (b.head.get(key).equals(a.get(key)))
            curr;
          else {
            find(a, b.tail, key, curr + 1)
          }
        } else {
          -1
        }
      }

      @tailrec
      def aux(key: String, a: List[Map[String, String]], c: List[Map[String, String]], head: List[String]): List[Map[String, String]] = {

        if (a.nonEmpty) {
          val temp = a.head;
          if (find(a.head, a.tail, key, 0) != -1) {
            val idx = find(a.head, a.tail, key, 0)

            aux(key, a.tail.patch(idx, Nil, 1), merge(temp, a.tail(idx), head, Map("" -> ""), 0) :: c, head)
          } else {
            val nou: Map[String, String] = create(temp, head, Map("" -> ""), 0)

            aux(key, a.tail, nou :: c, head)
          }
        }
        else
          c
      }

      aux(key, a, List.empty[Map[String, String]], head)
    }


    def createTable(almost: List[Map[String, String]], head: List[String]): List[List[String]] = {
      @tailrec
      def auxx(a: Map[String, String], head: List[String], idx: Int, acc: List[String]): List[String] = {
        if (idx == head.length)
          acc
        else {
          auxx(a, head, idx + 1, a.get(head(idx)).get :: acc)
        }
      }

      @tailrec
      def aux(almost: List[Map[String, String]], idx: Int, head: List[String], acc: List[List[String]]): List[List[String]] = {
        if (idx == almost.length)
          acc
        else {
          val line: List[String] = auxx(almost(idx), head, 0, List.empty[String])
          aux(almost, idx + 1, head, line.reverse :: acc)
        }
      }

      aux(almost, 0, head, List[List[String]]()).reverse
    }


  val rows1 = getTabular.map(el => columnNames.zip(el).toMap)
  val rows2 = other.getTabular.map(el => other.getColumnNames.zip(el).toMap)
    val headd = (getColumnNames ++ other.getColumnNames).distinct
  val almost = function("Language", rows1++rows2, headd)
    if((this.columnNames.contains(key)) && (other.getColumnNames.contains(key)))
      Some(new Table(headd, createTable(almost, headd)))
    else
      None
  }
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val nume = s.split("\n").toList.head.split(",").toList
    val corp = s.split("\n").toList.tail.map(_.split(",", -1).toList)
    new Table(nume, corp)
  }

}

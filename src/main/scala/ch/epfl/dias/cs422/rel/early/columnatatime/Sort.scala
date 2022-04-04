package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val data: IndexedSeq[Tuple] =
      input.execute().transpose.filter(_.last.asInstanceOf[Boolean])
    data
      .sorted(OrderingTuple)
      .slice(
        offset.getOrElse(0),
        offset.getOrElse(0) + fetch.getOrElse(data.length)
      )
      .transpose
      .map(toHomogeneousColumn)
  }

  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */
  object OrderingTuple extends Ordering[Tuple] {
    override def compare(x: Tuple, y: Tuple): Int =
      comparewith(x, y, collation.getFieldCollations.asScala.toList)
    @tailrec
    def comparewith(x: Tuple, y: Tuple, rels: List[RelFieldCollation]): Int =
      rels match {
        case Nil          => 0
        case head :: tail =>
          // transform tuple(coll.getFieldIndex) into Comparable for each coll
          val a = x(head.getFieldIndex).asInstanceOf[Comparable[Elem]]
          val b = y(head.getFieldIndex).asInstanceOf[Comparable[Elem]]
          val cmp =
            if (head.direction.isDescending)
              RelFieldCollation.compare(b, a, 0: Int)
            else RelFieldCollation.compare(a, b, 0: Int)
          cmp match {
            case 0   => comparewith(x, y, tail)
            case res => res
          }
      }
  }
}

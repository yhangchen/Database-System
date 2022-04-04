package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */
  private var sorted: Iterator[Tuple] = Iterator()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    sorted = input.toList
      .sorted(OrderingTuple)
      .iterator
      .slice(
        offset.getOrElse(0),
        offset.getOrElse(0) + fetch.getOrElse(input.toList.length)
      )
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    if (sorted.hasNext) Some(sorted.next()) else NilTuple

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}

  object OrderingTuple extends Ordering[Tuple] {
    override def compare(x: Tuple, y: Tuple): Int = {
      comparewith(x, y, collation.getFieldCollations.asScala.toList)
    }
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

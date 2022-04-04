package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
    * @inheritdoc
    */
  private var hashTable: Map[Int, List[Tuple]] = Map()
  private var matched: List[Tuple] = Nil

  override def open(): Unit = {
    // read left
    right.open()
    hashTable =
      left.toList.groupBy(tuple => getLeftKeys.map(tuple(_)).hashCode())
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    matched match {
      case head :: tail => // output concat tuples
        matched = tail
        Some(head)
      case Nil => // no matched found, search in hashTable
        right.next() match {
          case Some(t: Tuple) =>
            matched = hashTable.get(getRightKeys.map(t(_)).hashCode()) match {
              case Some(left_tuples) => left_tuples.map(_ :++ t)
              case None              => Nil
            }
            next()
          case _ => NilTuple
        }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = right.close()
}

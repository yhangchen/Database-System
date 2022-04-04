package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val left_data: IndexedSeq[Tuple] = left
      .execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1)) // only drop left_data Boolean
    val right_data: IndexedSeq[Tuple] = right
      .execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])
    val hashTable: Map[Int, IndexedSeq[Tuple]] =
      left_data.groupBy(tuple => getLeftKeys.map(tuple(_)).hashCode())
    right_data
      .flatMap(tuple =>
        hashTable.get(getRightKeys.map(tuple(_)).hashCode()) match {
          case Some(left_tuples) => left_tuples.map(_ :++ tuple)
          case _                 => IndexedSeq()
        }
      )
      .transpose
      .map(toHomogeneousColumn)
  }
}

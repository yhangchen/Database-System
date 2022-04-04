package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class LateJoin(
    left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {

  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
    * @inheritdoc
    */
  private var matched: List[LateTuple] = Nil
  private var hashTable: Map[Int, List[LateTuple]] = Map()
  private var left_size: Int = 0

  override def open(): Unit = {
    right.open()
    left_size = left.size
    hashTable = left.toList.groupBy(latetuple =>
      getLeftKeys.map(latetuple.value(_)).hashCode()
    )
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    matched match {
      case head :: tail => // output concat tuples
        matched = tail
        Some(head)
      case Nil => // no matched found, search in hashTable
        right.next() match {
          case Some(t: LateTuple) =>
            matched =
              hashTable.get(getRightKeys.map(t.value(_)).hashCode()) match {
                case Some(left_tuples: List[LateTuple]) =>
                  left_tuples.map(late =>
                    LateTuple(
                      late.vid + (t.vid - 1) * left_size,
                      late.value :++ t.value
                    )
                  )
                case _ => Nil
              }
            next()
          case _ => NilLateTuple
        }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = right.close()
}

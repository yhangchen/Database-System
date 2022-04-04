package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Stitch]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class Stitch protected (
    left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
) extends skeleton.Stitch[
      ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
    ](left, right)
    with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {

  /**
    * @inheritdoc
    */
  private var left_data: Map[Long, IndexedSeq[Any]] = Map()
  override def open(): Unit = {
    right.open()
    left_data = left.toList.map(tuple => tuple.vid -> tuple.value).toMap
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    right.next() match {
      case Some(y: LateTuple) =>
        left_data.get(y.vid) match {
          case Some(x) => Some(LateTuple(y.vid, x :++ y.value))
          case _       => next()
        }
      case _ => NilLateTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = right.close()
}

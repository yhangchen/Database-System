package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  /**
    * @inheritdoc
    */
  private var data: IndexedSeq[Tuple] = IndexedSeq()
  private var grouped: List[(Tuple, IndexedSeq[Tuple])] = List()

  override def open(): Unit = {
    data = input.toIndexedSeq
    if (data.isEmpty && groupSet.isEmpty) {
      grouped = Map(
        IndexedSeq[Elem]() -> IndexedSeq(aggCalls.map(_.emptyValue))
      ).toList
    } else {
      grouped = data
        .groupBy(tuple => groupSet.toArray.toIndexedSeq.map(tuple(_)))
        .toList
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    grouped match {
      case (key: Tuple, tuples: IndexedSeq[Tuple]) :: tail =>
        grouped = tail
        Some(
          key ++
            aggCalls.map(agg =>
              tuples
                .map(e => agg.getArgument(e))
                .reduce((e1, e2) => agg.reduce(e1, e2))
            )
        )
      case _ => NilTuple
    }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}
}

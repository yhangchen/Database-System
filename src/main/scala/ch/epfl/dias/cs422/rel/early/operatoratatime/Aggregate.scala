package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

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
  override def execute(): IndexedSeq[Column] = {
    val data: IndexedSeq[Tuple] =
      input.execute().transpose.filter(_.last.asInstanceOf[Boolean])
    if (data.isEmpty && groupSet.isEmpty) {
      IndexedSeq(aggCalls.map(_.emptyValue) :+ true).transpose
    } else {
      val groupInd: IndexedSeq[Int] = groupSet.toArray.toIndexedSeq
      data
        .groupBy(tuple => groupInd.map(tuple(_)))
        .map {
          case (key, tuples) =>
            key.++(
              aggCalls.map(agg =>
                tuples
                  .map(e => agg.getArgument(e))
                  .reduce((e1, e2) => agg.reduce(e1, e2))
              )
            ) :+ true
        }
        .toIndexedSeq
        .transpose
    }
  }
}

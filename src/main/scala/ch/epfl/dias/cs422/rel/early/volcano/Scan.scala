package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store._
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  private var prog = getRowType.getFieldList.asScala.map(_ => 0)
  private var ind: Int = 0
  private var maxInd: Long = 0.toLong

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    ind = 0
    maxInd = scannable.getRowCount
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (ind >= maxInd) NilTuple
    else {
      val tuple: Tuple = scannable match {
        case rows: RowStore => rows.getRow(ind)
//        case cols: ColumnStore =>
//          (0 until table.getRowType.getFieldCount).map(i =>
//            unwrap[Tuple](cols.getColumn(i))(ind)
//          )
//        case paxs: PAXStore =>
//          val minipage_size = paxs.getPAXPage(0).head.size
//          paxs
//            .getPAXPage(ind / minipage_size) // PAXPage = List[PAXMinipage]
//            .map(m => m(ind % minipage_size))
        case _ => null // not implemented
      }
      ind += 1
      Some(tuple)
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}
}

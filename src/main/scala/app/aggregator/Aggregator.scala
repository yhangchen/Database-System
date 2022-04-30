package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {
  type AggValue = (Option[(Double, Int)], (String, List[String]))
  type AggState = RDD[(Int, AggValue)]
  // (title id, (Option[(rating_sum_per_title, rating_num_per_title)], (title, keywords)))
  var state: AggState = _ // better than null init

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    val tmp = title.map(t => (t._1, (t._2, t._3)))
    state = ratings.map(t => (t._2, t._4)).aggregateByKey((0.0, 0))(
      (k, v) => (k._1 + v, k._2 + 1),
      (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).
      rightOuterJoin(tmp).persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = state.map(_._2).map(t =>
    (t._2._1, t._1 match {
      case Some(x) => 1.0 * x._1 / x._2
      case None => 0.0
    }))

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    val all_titles: RDD[Option[(Double, Int)]] = state.filter(t => keywords.forall(k => t._2._2._2.contains(k)))
      .map(t => t._2._1)
    if (all_titles.collect.length == 0) -1.0 else {
      val all_rated_titles: RDD[Double] = all_titles.filter(_.isDefined).map { case Some(x) => 1.0*x._1/x._2 }
      if (all_rated_titles.collect.length == 0) 0.0 else all_rated_titles.mean
    }
  }


  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta_ Delta ratings that haven't been included previously in aggregates
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val delta_simplify: Map[Int, Array[(Option[Double], Double)]] = delta_.map(t => (t._2, (t._3, t._4))).groupBy(_._1)
      .map(t => t._1 -> t._2.map(_._2))
    val new_state: AggState = state.map(t => {
      var value_sum: Double = 0.0
      var value_num: Int = 0
      delta_simplify.get(t._1) match {
        case Some(values) =>
          for (value <- values) {
            value match {
              case (Some(r1: Double), r2: Double) => value_sum += r2 - r1
              case (None, r2: Double) =>
                value_sum += r2
                value_num += 1
            }
          }
          t._2._1 match {
            case Some(x) => (t._1, (Some((value_sum + x._1, value_num + x._2)), t._2._2))
            case None => (t._1, (Some((value_sum, value_num)), t._2._2))
          }
        case None => t
      }
    }
    )

    state = state.unpersist()
    state = new_state.persist()
  }
}

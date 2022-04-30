package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.spark_project.jetty.util.HttpCookieStore.Empty

/**
 * Class for performing LSH lookups (enhanced with cache)
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookupWithCache(lshIndex : LSHIndex) extends Serializable {
  var cache: Map[IndexedSeq[Int], List[(Int, String, List[String])]] =
    Map[IndexedSeq[Int], List[(Int, String, List[String])]]()
  var lookup_hist: Map[IndexedSeq[Int], Int] =
    Map[IndexedSeq[Int], Int]()

  /**
   * The operation for building the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc : SparkContext) = {
    val total: Int = lookup_hist.values.sum
    val valid_keys: Array[IndexedSeq[Int]] = lookup_hist.filter{case (_,v)=> 1.0*v/total>0.01}.keys.toArray
    // occur in more that 1% (> 1%) of the queries
    val ext = sc.broadcast(lshIndex.getBuckets().filter(t=>valid_keys.contains(t._1)).collect().toMap)
    // broadcast to workers. Can not directly broadcast RDDs; instead, call collect() and broadcast the result.
    cache = ext.value
    lookup_hist = Map[IndexedSeq[Int], Int]() //  histogram data structure is reset
  }

  /**
   * Testing operation: force a cache based on the given object
   *
   * @param ext A broadcast map that contains the objects to cache
   */
  def buildExternal(ext : Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]]): Unit =
    cache = ext.value

  /**
   * Lookup operation on cache
   *
   * @param queries The RDD of keyword lists
   * @return The pair of two RDDs
   *         The first RDD corresponds to queries that result in cache hits and
   *         includes the LSH results
   *         The second RDD corresponds to queries that result in cache hits and
   *         need to be directed to LSH
   */
  def cacheLookup(queries: RDD[List[String]])
  : (RDD[(List[String], List[(Int, String, List[String])])], RDD[(IndexedSeq[Int], List[String])]) = {
    val query_hash = lshIndex.hash(queries)
    query_hash.collect().groupBy(_._1).mapValues(_.length).foreach{
      case (k,v) => lookup_hist += (k -> (v + lookup_hist.getOrElse(k, 0))) // key occurrence
    }

    val all_res: RDD[((IndexedSeq[Int], List[String]), Option[List[(Int, String, List[String])]], Int)] =
      query_hash.map(key => cache.get(key._1) match {
      case Some(x) => (key, Some(x), 1)
      case None => (key, None, 2)
    })
    val res = (all_res.filter(x=>x._3==1).map(x=>(x._1._2, x._2.get)), all_res.filter(x=>x._3==2).map(_._1))
    val length1: Int = res._1.collect().length
    if (length1==0) (null, res._2) // if the first item is empty, replace it with null
    else res
  }

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val rdd0 = this.cacheLookup(queries)
    val rdd2: RDD[(IndexedSeq[Int], List[String])] = rdd0._2
    val rdd3: RDD[(List[String], List[(Int, String, List[String])])] = lshIndex.lookup(rdd2).map(x=>(x._2, x._3))
    if (rdd0._1 != null) rdd0._1.union(rdd3) else rdd3
  }
}

# CS422 Project 1 - Apache Spark
Yihang Chen yihang.chen@epfl.ch

## Task 1: Bulk-load data
The implementation of the  `load()` methods for `TitlesLoader` and `RatingsLoader` is based on
```scala
sc.textFile(path).map(x => x.split('|')).map(...)
```

### Task 2: Average rating pipeline

#### Task 2.1: Rating aggregation
We define our aggregation type `AggState=(title_id: Int, (Option[(rating_sum_per_title: Double, rating_num_per_title: Int)], (title: String, 
keywords: List[String])))` 
and store it in `state`. When calling `getResult()`, we divide the `rating_sum_per_title` by `rating_num_per_title`
and get the average rating per title. 

To compute `(rating_sum_per_title, rating_num_per_title)` given `title_id`, we use
```scala
(title_id: Int, rating: Double).aggregateByKey((0.0, 0))(
  (k, v) => (k._1 + v, k._2 + 1), 
  (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
```

#### Task 2.2: Ad-hoc keyword rollup
The getKeywordQueryResult() implements queries over the aggregated ratings. We perform
two filtering. The first one select those which contains given keywords, and the second one select 
those which have been rated. We use `collect` to convert `RDD` to `Arrary`.

#### Task 2.3: Incremental maintenance
- **Step 1**. Simplify the input as `Map[title_id: Int, Array[(old_rating: Option[Double], new_rating Double)]]`, i.e.
a mapping from `title_id` to the `Array` of both old and new ratings. 
- **Step 2**. Scan over `state` to modify rows inside to produce a new `new_state: AggState`. For a given title, there are one to many `(old_rating: Option[Double], new_rating Double)` pairs. 
If the pair matches `(Some(r1: Double), r2: Double)`, i.e. a user's update of his own rating, the number of ratings are not changed
and the difference of ratings (`r2 - r1`) is added to the sum. Otherwise, the pair matches `(None, r2: Double)`, the number of ratings is added by 1
and the new rating (`r2`) is added to the sum.  
- **Step 3**. In **Step 2**, if the `state` does not contain the pair `(rating_sum_per_title: Double, rating_num_per_title: Int)`, 
we set `rating_sum_per_title=0.0` and `rating_num_per_title=0`.
- **Step 4**. We need to `unpersist` the old `state` and persist the `new_state`.


## Task 3: Similarity-search pipeline
#### Task 3.1: Indexing the dataset
The implementation is direct: `data.groupBy(t => minhash.hash(t._3))`. Since the `groupBy` produce 
`RDD[(K, Iterable[T])]`. We change `Iterable` to `List` by `map`.
#### Task 3.2: Near-neighbor lookups
In `NNLookup.scala`, we implement `lookup` method of `LSHIndex`. We get the bucket by 
`this.getBuckets()`. However, we cannot directly use `rdd.lookup` in the `map` clause, which will raise the error: `SPARK-5063 RDD transformations and actions can only be invoked by the driver`.
If we use something like `rdd1.map(rdd2.lookup(_._1))`, `rdd2.lookup` will be invoked on a worker node. 

Since the key of bucket is unique, we transform it into a map as workaround. However, since the `map` will not handle non-existing key automatically, 
we use `getOrElse` to return empty list in return. 

#### Task 3.3: Near-neighbor cache
The cache we use a map `cache = ext.value`, and initialized with an empty map. In the `cacheLookup`,
we set a flag to be 1 if the cache hits and 2 if the cache misses. 
Specifically, we first transform `queries: RDD[List[String]]` to `RDD[((IndexedSeq[Int], List[String]), Option[List[(Int, String, List[String])]], Int)]`
, i.e. `key, value, flag`, and use the flag to divide the RDD into two parts. In order to pass the test, we replace the 
first RDD with `null` if it is an empty RDD. 
#### Task 3.4: Cache policy
We use a global `lookup_hist: Map[IndexedSeq[Int], Int]` to record the "signature-occurrence" map. In the `cacheLookup`,
we update the `lookup_hist` by counting the signatures in the input `query_hash`.  We use
```scala
map1.foreach{ 
  case (k,v) => map2 += (k -> (v + map2.getOrElse(k, 0))) // key occurrence
}
```
In the `build` function, we use `valid_keys` to filter (i.e. `.filter{case (_,v)=> 1.0*v/total>0.01}`) the 
satisfied keys. Then, we broadcast the corresponding buckets (`lshIndex.getBuckets().filter(t=>valid_keys.contains(t._1))`) 
to data nodes. Note that we need to first `collect` then `sc.broadcast`.  In the end, we reset the histogram data structure.

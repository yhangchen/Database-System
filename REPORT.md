# CS422 Project 1 - Relational Operators and Execution Models
Yihang Chen yihang.chen@epfl.ch
## Task 1: Implement a volcano-style tuple-at-a-time engine
#### 1. Scan
For `scan`, we iterate over `ind` to produce the `ind`-th row for the RowStore database.
```scala
scannable match {
        case rows: RowStore => rows.getRow(ind)
}
```
where the total number of row is `scannable.getRowCount`.
#### 2. Filter
We match `input.next()` with `Some(x)` and filter with `predicate(x)`. 
#### 3. Project
We match `input.next()` with `Some(x)` and project to `evaluator(x)`. 
#### 4. Join
We read all inputs from the left `left.toList` and build a hash table `groupBy(tuple => getLeftKeys.map(tuple(_)).hashCode())`. 
The `matched` tuples are initialized to empty. 

When `matched` is empty, scan the next item of `right`, denoted as `t`, find the corresponding tuples in the `hashtable` by `hashTable.get(getRightKeys.map(t(_)).hashCode())`, and concatenate then with `t` by `left_tuples.map(_ :++ t)`.

When `matched` is not empty, pop the first item of `matched`.

#### 5. Sort
We extend `Ordering[Tuple]` to `OrderingTuple`, and use `.sorted(OrderingTuple)` to sort the input in `input()`. 

The skeleton code of extending `Ordering[T]` is 
```scala
  object OrderingTuple extends Ordering[T] {
    override def compare(x: T, y: T): Int = {
      ...
      // output 1 when x > y, -1 when x < y, 0 otherwise.
    }
```

The list of relations `rels` is obtained by `collation.getFieldCollations.asScala.toList`. We use a subroutine `comparewith`:
```scala
def comparewith(x: Tuple, y: Tuple, rels: List[RelFieldCollation]): Int = ???
```
In the `comparewith`, we first need to transform `Tuple` into `Comparable[Elem]` by `x(rel.getFieldIndex).asInstanceOf[Comparable[Elem]]`, 
where `rel` is a relation in `rels`. We iteratively apply `rel` in `rels` to `Comparable[Elem]`, if one `rel` can determine the ordering, 
the order of `x` and `y` is determined. Otherwise, we consider the next `rel` in `rels`.  Besides, when `rel.direction.isDescending`, the 
comparison needs to be reversed. To deal with `null` values, we use `RelFieldCollation.compare(c1, c2, 0:Int)` instead of `c1.compareTo(c2)`. 

Since `offset` and `fetch` is `Option[Int]`, in practice, we use `offset.getOrElse(0)` and `fetch.getOrElse(input.toList.length)`, i.e. the default
`offset` is zero and the default behavior of `fetach` is to fetch all the data. 

#### 6. Aggregate
In `input()`, we scan the full input: `data = input.toIndexedSeq`. If the `data` and the `groupSet` is both empty, the key of `grouped` is empty 
and corresponding value is `emptyValue`. Otherwise, we group `data` by `grouped = data.groupBy(tuple => groupSet.toArray.toIndexedSeq.map(tuple(_))).toList`

In `next()`, for `(key, tuples)` in `grouped`, we concatenate key with reduced value: `key ++ aggCalls.map(agg =>tuples.map(e => agg.getArgument(e)).reduce((e1, e2) => agg.reduce(e1, e2)))`.

## Task 2: Late Materialization
### Subtask 2.A: Implement Late Materialization primitive operators
#### 1. Drop
We only need to match `input.next()` with `Some(x)` and output `Some(x.value)`.
#### 2. Stitch
We first scan the full left input and transform it into a map: `left.toList.map(tuple => tuple.vid -> tuple.value).toMap`. 
Then, we use the mapping to match the right `LateTuple`'s vid with a left `LateTuple` and concatenate these two values together.
### Subtask 2.B: Extend relational operators to support execution on LateTuple data
#### 3. LateFilter
Similar to Task 1 `Filter`, but replace `predicate(x)` with `predicate(x.value)`.
#### 4. LateProject
Similar to Task 1 `Project`, but replace `evaluator(x)` with `LateTuple(x.vid, evaluator(x.value))`.
#### 5. LateJoin
Similar to Task 1 `Join`, but replace each `Tuple` with `LateTuple.value`. For the joined tuple's vid, 
we use `leftLateTuple.vid + (rightLateTuple.vid -1) * left.size`, where `left.size` is the size of the full left input.
## Task 3: Query Optimization Rules
### Subtask 3.A: Implement the Fetch operator
First, we can use `column.getElement(t.vid).getOrElse(None)` to fetch from the `column` in the position `t.vid`, where `t` is the
input `LateTuple`. Notice that fetched data could be either a `Tuple` or `Elem`. For the latter case, we transform it into a single
element `Tuple`. Then, we match the `project` to determine whether we perform further transformation on fetched data.
### Subtask 3.B: Implement the Optimization rules
#### 1. LazyFetchRule
Since we only consider logical plan, we construct a `LogicalFetch`. The given list
is `LogicalStitch`, `RelNode`, `LateColumnScan`. We want to replace stitch
with fetch. We use `call.rel` to access the given `RelNode`, so `cal.rel(0)` is `LogicalStitch` and so on.

Recall
```scala
object LogicalFetch {
  def create(
              input: RelNode,
              fetchType: RelDataType,
              column: LateStandaloneColumnStore,
              projects: Option[java.util.List[_ <: RexNode]],
              c: Class[_ <: LogicalFetch]
            ): LogicalFetch = {
    Construct.create(c, input, fetchType, column, projects)
  }
}
```
and we replace the items one by one. `input` should be any `RelNode`, i.e. `cal.rell(1)`, `fetchType` and
`column` should be obtained from the scan, i.e. `call.rel(2)`. We do not evaluate over reconstructed
columns, so `project` is `None`. `c` should be `classOf[LogicalFetch]`.
#### 2. LazyFetchFilterRule
The given list
is `LogicalStitch`, `RelNode`, `LogicalFilter`, `LateColumnScan`. In other words, the filtered scan is 
stitched with some relational node. What we want to do is to first fetch from the scan and then
perform filtering. 

The fetch node is constructed similarly with previous part. Then, we `copy` the `LogicalFilter` node
and put it on the top. However, we cannot directly use `filter.copy(filter.getTraitSet, fetch, filter.getCondition)`. 
Since after being fetched, the position of the filtered columns might change. Since we append the fetched columns
to the end, the position will add `node.getRowType.getFieldCount`. So, we use `RexUtil.shift` to obtain the new condition.


#### 3. LazyFetchProjectRule
The given list
is `LogicalStitch`, `RelNode`, `LogicalProject`, `LateColumnScan`. In other words, the filtered scan is
stitched with some relational node. What we want to do is to fetch from the `LateColumnScan` and directly perform projection during
fetching. Hence, `projects` in `LogicalFetch.create` is not `None`. Since we directly evaluate over fetched data, the position of the 
projection's input is not changed. Hence, unlike `LazyFetchProjectRule`, we directly use `project.getProjects`.

## Task 4: Execution Models
### Subtask 4.A: Enable selection-vectors in operator-at-a-time execution
The basic idea is to read all the input and use `transpose` to transform them into row-based storage. Since the last column is boolean, the last item in each tuple after transpose is therefore boolean. 
#### 1. Filter
We need to set the boolean part of one tuple to `true` iff the original boolean is `true` and the predicate is satisfied: `last
.map(_.asInstanceOf[Boolean]).zip(inputs.dropRight(1).transpose.map(predicate)).map(t => t._1 && t._2).asInstanceOf[Column]`. Note that we
need `asInstanceOf` to match the type.
#### 2. Project
The idea is the same, project the data part and append boolean part: `inputs.dropRight(1).transpose.map(evaluator).transpose :+ inputs.last`.
#### 3. Join
We scan all `left` and `right` input, and delete the boolean part in the left input, since we concatenate by `left ++ right`. We filter the tuple by its boolean part:
`filter(_.last.asInstanceOf[Boolean])` and use the code in Task 1 `Join`.
#### 4. Sort
We filter the tuple by its boolean part and use the code in Task 1 `Sort`.
#### 5. Aggregate
We filter the tuple by its boolean part and use the code in Task 1 `Aggregate`. Note that we need to append `true` to each generated tuple.

### Subtask 4.B: Column-at-a-time with selection vectors and mapping functions
For `Join`, `Sort`, `Aggregate`, we only need to add `map(toHomogeneousColumn)` in the end to map the `Column` to `HomogeneousColumn`.

For `Filter` and `Project`, we correspondingly use `mappredicate` and `evals` to avoid `transpose`. For the boolean `HomogeneousColumn`, we use `unwrap[Boolean](inputs.last)` to covert
it to a boolean array if necessary.
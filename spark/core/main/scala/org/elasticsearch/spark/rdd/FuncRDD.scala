package org.elasticsearch.spark.rdd

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}

/**
  * An RDD that applies the provided function to every partition of the parent RDD.
  */
private[spark] class FuncRDD[U: ClassTag, T: ClassTag](
                                                                 var prev: RDD[T],
                                                                 f: (TaskContext, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
                                                                 preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, firstParent[T].iterator(split, context))

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
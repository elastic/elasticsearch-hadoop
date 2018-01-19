package org.elasticsearch.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

class FuncRDD[T: ClassTag](
          var prev: RDD[T],
          f: (TaskContext, Iterator[T]) => Iterator[T],  // (TaskContext, partition index, iterator)
          preservesPartitioning: Boolean = false)
        extends RDD[T](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
          f(context, firstParent[T].iterator(split, context))

  override def clearDependencies() : Unit = {
      super.clearDependencies()
        prev = null
      }
}
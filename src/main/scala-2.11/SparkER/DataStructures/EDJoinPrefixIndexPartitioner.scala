package SparkER.DataStructures

import org.apache.spark.Partitioner

class EDJoinPrefixIndexPartitioner(numPart: Int) extends Partitioner {
  val load = Array.fill[Double](numPart)(0.0)

  override def getPartition(key: Any): Int = {
    val part = load.indexOf(load.min)
    val s = key.asInstanceOf[Array[(Int, Int, Array[(Int, Int)], String)]].length.toDouble
    load.update(part, load(part) + (s * s - 1))
    part
  }

  override def numPartitions: Int = numPart
}
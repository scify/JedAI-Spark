package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import org.apache.spark.rdd.RDD

trait EntityClusteringTrait {

  def getClusters(profiles: RDD[Profile],
                  edges: RDD[WeightedEdge],
                  maxProfileID: Int,
                  edgesThreshold: Double = 0,
                  separatorID: Int = -1
                 ): RDD[(Int, Set[Int])]
}

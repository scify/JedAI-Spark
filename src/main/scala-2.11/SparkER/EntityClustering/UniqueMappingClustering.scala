package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import SparkER.EntityClustering.EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.apache.spark.rdd.RDD

object UniqueMappingClustering extends EntityClusteringTrait {
  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int): RDD[(Int, Set[Int])] = {
    /** Generates the connected components */
    val cc = connectedComponents(edges.filter(_.weight > edgesThreshold))
    /** Then, in parallel, for each connected components computes the clusters */
    val res = cc.mapPartitions { partition =>
      /* Used to check if a profile was already added to a cluster */
      val visited = Array.fill[Boolean](maxProfileID + 1) {
        false
      }
      /* Generated clusters */
      val clusters = scala.collection.mutable.Map[Int, Set[Int]]()

      partition.foreach { cluster =>
        /* Sorts the elements in the cluster descending by their similarity score */
        val sorted = cluster.toList.sortBy(x => (-x._3, x._1))
        sorted.foreach { case (u, v, sim) =>
          if (!visited(u.toInt) && !visited(v.toInt)) {
            visited.update(u.toInt, true)
            visited.update(v.toInt, true)
            clusters.put(u, Set(u, v))
          }
        }
      }

      clusters.toIterator
    }
    addUnclusteredProfiles(profiles, res)
  }
}

package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.apache.spark.rdd.RDD

object ConnectedComponentsClustering extends EntityClusteringTrait {

  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double = 0, separatorID: Int = -1): RDD[(Int, Set[Int])] = {
    val cc = connectedComponents(edges.filter(_.weight >= edgesThreshold))
    val a = cc.map(x => x.flatMap(y => y._1 :: y._2 :: Nil)).zipWithIndex().map(x => (x._2.toInt, x._1.toSet))
    addUnclusteredProfiles(profiles, a)
  }
}
